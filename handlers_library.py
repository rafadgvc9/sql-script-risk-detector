import re
from typing import Dict, Any, List, Optional 
from functions_library import is_warehouse_xs, parse_object_name, _create_result

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
    
    # USE WAREHOUSE
    elif re.match(r"^USE\s+WAREHOUSE\s", stmt_clean):
        match = re.search(r"USE\s+WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
        if match:
            warehouse_name = match.group(1).strip('"').strip("'")
            current_context["warehouse"] = warehouse_name
            risk_level = "ALTA"
            if is_warehouse_xs():
                risk_level = "BAJA"

            obj_info = parse_object_name(warehouse_name) if warehouse_name else None
    
            if obj_info:
                obj_info["current_context"] = current_context.copy()
                if proc_context:
                    obj_info["inside_procedure"] = proc_context
            return [_create_result("USE_WAREHOUSE", warehouse_name, None, True, obj_info)]
    
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
    obj_name = None

    m = re.search(r"CALL\s+PROCEDURE\s+([A-Z0-9_.\"]+)\s*\(?", stmt_clean, re.IGNORECASE)
    if m:
        obj_name = m.group(1)
    else:
        m2 = re.search(r"CALL\s+([A-Z0-9_.\"]+)", stmt_clean, re.IGNORECASE)
        if m2:
            candidate = m2.group(1)
            if not candidate.startswith(":"):
                obj_name = candidate

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