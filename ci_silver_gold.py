import sqlparse
import random
import re
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Callable

# estas variables se deben de quitar, y se debe referenciar 
TEMPLATE_VARIABLES = {
    "current_environment": "PRO",
    "env": "PRO",
    "environment": "PRO",
    "region": "EU",
    "project": "ECI"
}

# definicion de riesgos para cada accion
RIESGO = {
    # DDL TABLAS 
    "CREATE_TABLE": "BAJA",                 
    "DROP_TABLE": ("ALTA", "MEDIA"),                   
    "CREATE_OR_REPLACE_TABLE": ("ALTA", "MEDIA"),      
    "CREATE_OR_ALTER_TABLE": ("MEDIA", "MEDIA"),       
    "UNDROP_TABLE": "BAJA",
    "TRUNCATE_TABLE": ("ALTA", "MEDIA"),
    "ALTER_TABLE_NOT_COLUMNS": ("ALTA", "MEDIA"),     

    # DML
    "INSERT_VALUES": ("MEDIA", "MEDIA"),                      
    "DELETE_VALUES": ("ALTA", "MEDIA"),                       
    "MERGE_VALUES": ("MEDIA", "MEDIA"),                       

    # DATABASE
    "CREATE_DATABASE": "BAJA", 
    "CREATE_OR_REPLACE_DATABASE": "ALTA", 
    "CREATE_OR_ALTER_DATABASE": "MEDIA", 
    "ALTER_DATABASE": "MEDIA",             
    "DROP_DATABASE": "ALTA",  
    "UNDROP_DATABASE": "BAJA", 

    # SCHEMA
    "CREATE_SCHEMA": "BAJA", 
    "CREATE_OR_REPLACE_SCHEMA": "ALTA", 
    "CREATE_OR_ALTER_SCHEMA": "MEDIA", 
    "ALTER_SCHEMA": "MEDIA",               
    "DROP_SCHEMA": "ALTA",  
    "UNDROP_SCHEMA": "BAJA", 

    # WAREHOUSE
    "CREATE_WAREHOUSE": "BAJA", 
    "CREATE_OR_REPLACE_WAREHOUSE": ("ALTA", "MEDIA"),
    "ALTER_WAREHOUSE": "ALTA",
    "CREATE_OR_ALTER_WAREHOUSE": ("ALTA", "MEDIA"),
    "DROP_WAREHOUSE": "ALTA",
    "USE_WAREHOUSE": "BAJA",

    # SHARE
    "CREATE_SHARE": "BAJA", 
    "ALTER_SHARE": "MEDIA", 
    "DROP_SHARE": "ALTA",  

    # VIEW
    "CREATE_VIEW": "BAJA",
    "CREATE_OR_ALTER_VIEW": ("MEDIA", "MEDIA"),        
    "ALTER_VIEW": ("MEDIA", "MEDIA"),                  
    "CREATE_OR_REPLACE_VIEW": ("ALTA", "MEDIA"),
    "DROP_VIEW": ("ALTA", "ALTA"), 

    # TAG
    "CREATE_TAG": "BAJA",                 
    "DROP_TAG": "ALTA",                   
    "CREATE_OR_REPLACE_TAG": "ALTA",      
    "CREATE_OR_ALTER_TAG": "MEDIA",       
    "UNDROP_TAG": "BAJA",
    "ALTER_TAG": "ALTA",                   

    # ALTER TABLE ALTER COLUMN
    "ALTER_TABLE_ADD_COLUMN": "MEDIA",
    "ALTER_TABLE_DROP_COLUMN": "ALTA",      
    "ALTER_TABLE_MODIFY_COLUMN_TYPE": "ALTA", 
    
    # PRIVILEGE & POLICY
    "GRANT_PRIVILEGE": "ALTA",
    "REVOKE_PRIVILEGE": "ALTA",
    "CREATE_ACCESS_POLICY": "ALTA",
    "ALTER_ACCESS_POLICY": "ALTA",
    "DROP_ACCESS_POLICY": "ALTA",

    # CONTEXT STATEMENTS
    "USE_DATABASE": "BAJA",
    "USE_SCHEMA": "BAJA",

    # PROCEDURE
    "CREATE_PROCEDURE": "BAJA",
    "DROP_PROCEDURE": "ALTA",
    "ALTER_PROCEDURE": "MEDIA",
    "CREATE_OR_REPLACE_PROCEDURE": ("ALTA", "MEDIA"),
    "CALL_PROCEDURE" : "ALTA",

    # TASK
    "CREATE_TASK": "BAJA",
    "DROP_TASK": "ALTA",
    "ALTER_TASK": "MEDIA",
    "EXECUTE_TASK": "ALTA",
    "CREATE_OR_REPLACE_TASK": ("ALTA", "MEDIA"),

    # RESOURCE MONITOR
    "CREATE_RESOURCE_MONITOR": "BAJA",
    "DROP_RESOURCE_MONITOR": "ALTA",
    "ALTER_RESOURCE_MONITOR": "MEDIA",
    "CREATE_OR_REPLACE_RESOURCE_MONITOR": ("ALTA", "MEDIA"),
}

def resolve_template_variables(text: str, variables: Dict[str, str] = None) -> Tuple[str, List[str]]:
    """
    Detecta y reemplaza variables de template en formato {{ variable }} o {variable}
    Retorna el texto resuelto y una lista de variables encontradas.
    """
    if variables is None:
        variables = TEMPLATE_VARIABLES
    
    detected_vars = []
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
        
        if var_value:
            # Reemplazar la variable con su valor
            resolved_text = resolved_text.replace(match.group(0), var_value)
        else:
            
            print(f"   ADVERTENCIA: Variable '{{{{ {var_name} }}}}' no encontrada en configuración")
    
    return resolved_text, detected_vars

def normalize_dynamic_sql(sql_string: str, template_vars: Dict[str, str] = None) -> str:
    """
    Normaliza sentencias sql con concatenaciones de casteos de variables
    """
    if template_vars is None:
        template_vars = TEMPLATE_VARIABLES
    
    vars_lower = {k.lower(): v for k, v in template_vars.items()}
    
    result = sql_string
    max_iterations = 30
    changed = True
    iteration = 0
    
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


# esta funcion debe cambiarse a la existente
def get_object_lineage():
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
    riesgo_base = RIESGO[accion]
    riesgo_final = ""
    
    if isinstance(riesgo_base, tuple) and needs_lineage_check:
        riesgo_con_linaje, riesgo_sin_linaje = riesgo_base
        if get_object_lineage():
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
    """
    Extrae el contenido de un procedimiento almacenado, delimitado por $$ o por comillas simples.
    """

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
    """
    Extrae sentencias SQL asignadas a variables en procedimientos de Snowflake
    """
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


def _handle_drop(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para sentencias DROP."""
    obj_type = ""
    if "TABLE" in stmt_clean: obj_type = "TABLE"
    elif "VIEW" in stmt_clean: obj_type = "VIEW"
    elif "SCHEMA" in stmt_clean: obj_type = "SCHEMA"
    elif "DATABASE" in stmt_clean: obj_type = "DATABASE"
    elif "WAREHOUSE" in stmt_clean: obj_type = "WAREHOUSE"
    elif "SHARE" in stmt_clean: obj_type = "SHARE"
    elif "TAG" in stmt_clean: obj_type = "TAG"
    elif "ACCESS_POLICY" in stmt_clean: obj_type = "ACCESS_POLICY"
    elif "TASK" in stmt_clean: obj_type = "TASK"
    elif "RESOURCE MONITOR" in stmt_clean: obj_type = "RESOURCE_MONITOR"
    elif "PROCEDURE" in stmt_clean: obj_type = "PROCEDURE"
    
    if not obj_type:
        return []
    
    match = re.search(fr"{obj_type.replace('_', ' ')}\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result(f"DROP_{obj_type}", obj_name, None, True, obj_info)]

def _handle_create(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para sentencias CREATE."""
    obj_type = ""
    if "VIEW" in stmt_clean: obj_type = "VIEW"
    elif "TABLE" in stmt_clean: obj_type = "TABLE"
    elif "TASK" in stmt_clean: obj_type = "TASK"
    elif "SCHEMA" in stmt_clean: obj_type = "SCHEMA"
    elif "DATABASE" in stmt_clean: obj_type = "DATABASE"
    elif "WAREHOUSE" in stmt_clean: obj_type = "WAREHOUSE"
    elif "SHARE" in stmt_clean: obj_type = "SHARE"
    elif "TAG" in stmt_clean: obj_type = "TAG"
    elif "ACCESS_POLICY" in stmt_clean: obj_type = "ACCESS_POLICY"
    elif "RESOURCE MONITOR" in stmt_clean: obj_type = "RESOURCE_MONITOR"
    
    if not obj_type:
        return []
    
    accion_base = f"CREATE_{obj_type}"
    needs_lineage_check = False
    
    if "OR REPLACE" in stmt_clean:
        accion_base = f"CREATE_OR_REPLACE_{obj_type}"
        needs_lineage_check = True
    elif "OR ALTER" in stmt_clean:
        accion_base = f"CREATE_OR_ALTER_{obj_type}"
        needs_lineage_check = True
    
    match = re.search(fr"{obj_type.replace('_', ' ')}\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result(accion_base, obj_name, None, needs_lineage_check, obj_info)]

def _handle_alter_table(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler específico para ALTER TABLE."""
    table_match = re.search(r"TABLE\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
    tabla = table_match.group(1) if table_match else None
    obj_info = parse_object_name(tabla) if tabla else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    if "ADD COLUMN" in stmt_clean:
        col_match = re.search(r"ADD\s+COLUMN\s+([A-Z0-9_\"]+)", stmt_clean)
        columna = col_match.group(1) if col_match else None
        return [_create_result("ALTER_TABLE_ADD_COLUMN", tabla, columna, False, obj_info)]
    elif "DROP COLUMN" in stmt_clean:
        col_match = re.search(r"DROP\s+COLUMN\s+([A-Z0-9_\"]+)", stmt_clean)
        columna = col_match.group(1) if col_match else None
        return [_create_result("ALTER_TABLE_DROP_COLUMN", tabla, columna, True, obj_info)]
    elif "ALTER COLUMN" in stmt_clean and "TYPE" in stmt_clean:
        col_match = re.search(r"ALTER\s+COLUMN\s+([A-Z0-9_\"]+)", stmt_clean)
        columna = col_match.group(1) if col_match else None
        return [_create_result("ALTER_TABLE_MODIFY_COLUMN_TYPE", tabla, columna, True, obj_info)]
    else:
        return [_create_result("ALTER_TABLE_NOT_COLUMNS", tabla, None, True, obj_info)]

def _handle_alter(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para sentencias ALTER."""
    if "TABLE" in stmt_clean:
        return _handle_alter_table(stmt_clean, current_context, proc_context)
    
    # Para otros tipos de ALTER
    obj_type_map = {
        "VIEW": "ALTER_VIEW",
        "DATABASE": "ALTER_DATABASE",
        "SCHEMA": "ALTER_SCHEMA",
        "WAREHOUSE": "ALTER_WAREHOUSE",
        "SHARE": "ALTER_SHARE",
        "TAG": "ALTER_TAG",
        "ACCESS_POLICY": "ALTER_ACCESS_POLICY",
        "TASK": "ALTER_TASK",
        "RESOURCE MONITOR": "ALTER_RESOURCE_MONITOR",
        "PROCEDURE": "ALTER_PROCEDURE"
    }
    
    for obj_keyword, action in obj_type_map.items():
        if obj_keyword in stmt_clean:
            match = re.search(fr"{obj_keyword}\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
            obj_name = match.group(1) if match else None
            obj_info = parse_object_name(obj_name) if obj_name else None
            
            if obj_info:
                obj_info["current_context"] = current_context.copy()
                if proc_context:
                    obj_info["inside_procedure"] = proc_context
            
            return [_create_result(action, obj_name, None, True, obj_info)]
    
    return []

def _handle_insert(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para INSERT."""
    match = re.search(r"INSERT\s+INTO\s+([A-Z0-9_.\"]+)(?=\s*[\(]|\s+VALUES)", stmt_clean)
    print (match.group)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("INSERT_VALUES", obj_name, None, True, obj_info)]

def _handle_delete(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para DELETE."""
    match = re.search(r"DELETE\s+FROM\s+([A-Z0-9_.\"]+)(?=\s+(?:WHERE|USING)|\s*;|\s*$)", stmt_clean)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("DELETE_VALUES", obj_name, None, True, obj_info)]

def _handle_merge(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para MERGE."""
    match = re.search(r"MERGE\s+INTO\s+([A-Z0-9_.\"]+)(?:\s+(?:AS\s+)?[A-Z0-9_\"]+)?\s+USING", stmt_clean, re.IGNORECASE)
    if not match:
        match = re.search(r"MERGE\s+INTO\s+([A-Z0-9_.\"]+)", stmt_clean, re.IGNORECASE)
    obj_name = match.group(1) if match else None
    print(f"Nombre de objeto detectado: {obj_name}")
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("MERGE_VALUES", obj_name, None, True, obj_info)]

def _handle_truncate(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para TRUNCATE."""
    match = re.search(r"TABLE\s+([A-Z0-9_.\"]+)(?=\s*;|\s*$)", stmt_clean)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("TRUNCATE_TABLE", obj_name, None, True, obj_info)]

def _handle_undrop(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para UNDROP."""
    obj_type = ""
    if "TABLE" in stmt_clean: obj_type = "TABLE"
    elif "SCHEMA" in stmt_clean: obj_type = "SCHEMA"
    elif "DATABASE" in stmt_clean: obj_type = "DATABASE"
    elif "TAG" in stmt_clean: obj_type = "TAG"
    
    if not obj_type:
        return []
    
    match = re.search(fr"{obj_type}\s+([A-Z0-9_.\"]+)(?=\s*;|\s*$)", stmt_clean)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result(f"UNDROP_{obj_type}", obj_name, None, False, obj_info)]

def _handle_grant(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para GRANT."""
    match = re.search(r"GRANT\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)(?=\s+TO)", stmt_clean)
    if not match:
        return []
    
    obj_name = match.group(2)
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("GRANT_PRIVILEGE", obj_name, None, True, obj_info)]

def _handle_revoke(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para REVOKE."""
    match = re.search(r"REVOKE\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)(?=\s+FROM)", stmt_clean)
    if not match:
        return []
    
    obj_name = match.group(2)
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("REVOKE_PRIVILEGE", obj_name, None, False, obj_info)]

def _handle_use(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para USE DATABASE/SCHEMA."""
    # USE DATABASE
    if re.match(r"^USE\s+DATABASE\s+", stmt_clean):
        match = re.search(r"^USE\s+(?:DATABASE\s+)?([A-Z0-9_.\"]+)", stmt_clean)
        if match:
            db_name = match.group(1).strip('"').strip("'")
            current_context["database"] = db_name
            current_context["schema"] = None
            return [_create_result("USE_DATABASE", db_name, None, False, 
                                  {"context": "database", "value": db_name})]
    
    # USE SCHEMA
    elif re.match(r"^USE\s+SCHEMA\s", stmt_clean):
        match = re.search(r"^USE\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
        if match:
            full_name = match.group(1).strip('"').strip("'")
            parts = full_name.split('.')
            
            if len(parts) == 2:
                current_context["database"] = parts[0]
                current_context["schema"] = parts[1]
            elif len(parts) == 1:
                current_context["schema"] = parts[0]
            
            return [_create_result("USE_SCHEMA", full_name, None, False, 
                                  {"context": "schema", 
                                   "database": current_context["database"],
                                   "schema": current_context["schema"]})]
    
    # USE (equivalente a USE DATABASE)
    elif re.match(r"^USE\s+[A-Z0-9_.\"]", stmt_clean):
        match = re.search(r"^USE\s+([A-Z0-9_.\"]+)", stmt_clean)
        if match:
            db_name = match.group(1).strip('"').strip("'")
            current_context["database"] = db_name
            current_context["schema"] = None
            return [_create_result("USE_DATABASE", db_name, None, False, 
                                  {"context": "database", "value": db_name})]
    
    return []

def _handle_execute(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para EXECUTE."""
    match_task = re.search(r"EXECUTE\s+TASK\s+([A-Z0-9_.\"]+)", stmt_clean, re.IGNORECASE)
    if match_task:
        obj_name = match_task.group(1)
        obj_info = parse_object_name(obj_name)
        if obj_info:
            obj_info["current_context"] = current_context.copy()
            if proc_context:
                obj_info["inside_procedure"] = proc_context
        return [_create_result("EXECUTE_TASK", obj_name, None, False, obj_info)]
    
    match_proc = re.search(r"EXECUTE\s+([A-Z0-9_.\"]+)\s*\(", stmt_clean, re.IGNORECASE)
    if match_proc:
        obj_name = match_proc.group(1)
        obj_info = parse_object_name(obj_name)
        if obj_info:
            obj_info["current_context"] = current_context.copy()
            if proc_context:
                obj_info["inside_procedure"] = proc_context
        return [_create_result("EXECUTE_PROCEDURE", obj_name, None, False, obj_info)]
    
    return []

def _handle_call(stmt_clean: str, current_context: Dict, proc_context: Optional[str] = None) -> List[Dict[str, Any]]:
    """Handler para CALL."""
    match = re.search(r"CALL\s+PROCEDURE\s+([A-Z0-9_.\"]+)(?=\s*;|\s*$)", stmt_clean)
    print(stmt_clean)
    obj_name = match.group(1) if match else None
    obj_info = parse_object_name(obj_name) if obj_name else None
    
    if obj_info:
        obj_info["current_context"] = current_context.copy()
        if proc_context:
            obj_info["inside_procedure"] = proc_context
    
    return [_create_result("CALL_PROCEDURE", obj_name, None, True, obj_info)]


STATEMENT_HANDLERS = [
    (r"^USE\s+", _handle_use),
    (r"^CREATE", _handle_create),
    (r"^ALTER", _handle_alter),
    (r"^DROP", _handle_drop),
    (r"^UNDROP", _handle_undrop),
    (r"^TRUNCATE\s+TABLE", _handle_truncate),
    (r"^INSERT\s+INTO", _handle_insert),
    (r"^MERGE\s+INTO", _handle_merge),
    (r"^DELETE\s+FROM", _handle_delete),
    (r"^GRANT\s+", _handle_grant),
    (r"^REVOKE\s+", _handle_revoke),
    (r"^EXECUTE\s+", _handle_execute),
    (r"^CALL\s+", _handle_call),
]

# funcion principal para analizar todo el script 
def analizar_sql(path_sql: str, template_vars: Dict[str, str] = None):
    sql_text = Path(path_sql).read_text()
    resolved_sql, all_detected_vars = resolve_template_variables(sql_text, template_vars)
    
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
        stmt_normalized = normalize_dynamic_sql(stmt_uncommented, template_vars)
        stmt_clean = '\n'.join(cleaned_lines).strip().upper()
        stmt_clean = stmt_normalized.upper()
        
        if not stmt_clean:
            continue
        
        

        if re.match(r"^CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE", stmt_clean):
            match = re.search(r"PROCEDURE\s+([A-Z0-9_.\"]+)\s*\(", stmt_clean)
            proc_name = match.group(1) if match else None
            
            # extrae las sentencias del procedimiento 
            proc_body = extract_procedure_body(stmt_clean)
            if proc_body:
                
                # pasa por todas las variables de texto por si tienen sentencias guardadas
                variable_sqls = extract_sql_from_variables(proc_body, template_vars)
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
                    inner_stmt_clean = '\n'.join(inner_cleaned).strip().upper()
                    
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
    for pattern, handler in STATEMENT_HANDLERS:
        if re.match(pattern, stmt_clean):
            return handler(stmt_clean, current_context, proc_context)
    
    return []

def analizar_multiples_archivos(archivos_sql: List[str] = None, 
                                template_vars: Dict[str, str] = None) -> int:
    if archivos_sql is None:
        # si no se 
        sql_files = []
        for root, dirs, files in os.walk("."):
            for file in files:
                if file.endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
        sql_files = [f for f in sql_files if not any(part.startswith('.') for part in Path(f).parts)]
        sql_files = sql_files[:10] 
    else:
        sql_files = archivos_sql
    
    if not sql_files:
        print("No se encontraron archivos SQL para analizar")
        return 0
    
    total_risk = False
    risky_files = []
    
    for sql_file in sql_files:
        try:
            riesgo, resultados = analizar_sql(sql_file, template_vars)
            
            if resultados:
                if riesgo:
                    total_risk = True
                    risky_sentences = [r for r in resultados if r["riesgo"] in ["MEDIA", "ALTA"]]
                    risky_files.append({
                        'file': sql_file,
                        'sentences': risky_sentences
                    })
                
        except Exception as e:
            print(f"Error analizando {sql_file}: {str(e)}\n")
            return 1
    
    if total_risk:
        print("\nSe han detectado operaciones con riesgo")
        
        for archivo_info in risky_files:
            print(f"\nArchivo: {archivo_info['file']}")
            print(f"   Total de operaciones con riesgo: {len(archivo_info['sentences'])}\n")
            
            for i, sentence_info in enumerate(archivo_info['sentences'], 1):
                print(f"\n Operación {i} - Riesgo: {sentence_info['riesgo']}")
                print(f"   Acción: {sentence_info['accion']}")
                if sentence_info['objeto']:
                    print(f"   Objeto: {sentence_info['objeto']}")
                if sentence_info['columna']:
                    print(f"   Columna: {sentence_info['columna']}")
                
                if 'object_info' in sentence_info and sentence_info['object_info']:
                    obj_info = sentence_info['object_info']
                    print(f"   Nivel de cualificación: {obj_info.get('qualification_level', 'N/A')}")
                    if obj_info.get('database'):
                        print(f"   Database explícita: {obj_info['database']}")
                    if obj_info.get('schema'):
                        print(f"   Schema explícito: {obj_info['schema']}")
                    if obj_info.get('inside_procedure'):
                        print(f"   Dentro del procedimiento: {obj_info['inside_procedure']}")
                    if obj_info.get('from_variable'):
                        print(f"   Origen: Asignación de variable")
                    ctx = obj_info.get('current_context', {})
                    if ctx.get('database') or ctx.get('schema'):
                        print(f"   Contexto activo -> Database: {ctx.get('database', 'N/A')}, Schema: {ctx.get('schema', 'N/A')}")
                
        return 1
    else:
        print("   No se detectaron operaciones de alto riesgo")
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        archivos = sys.argv[1:]
        
        if len(archivos) == 1 and os.path.isdir(archivos[0]):
            exit_code = analizar_multiples_archivos(None)
            sys.exit(exit_code)
        else:
            sql_files = [f for f in archivos if f.endswith('.sql') and os.path.isfile(f)]
            
            if not sql_files:
                print("No se proporcionaron archivos SQL válidos")
                sys.exit(0)
            
            exit_code = analizar_multiples_archivos(sql_files)
            sys.exit(exit_code)
    else:
        exit_code = analizar_multiples_archivos(None)
        sys.exit(exit_code)
