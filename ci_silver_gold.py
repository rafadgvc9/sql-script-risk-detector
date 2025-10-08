import sqlparse
import random
import re
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Callable

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
    "INSERT_VALUES": ("MEDIA", "BAJA"),                      
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
    "ALTER_WAREHOUSE": "ALTA",
    "CREATE_OR_ALTER_WAREHOUSE": "ALTA",
    "DROP_WAREHOUSE": "ALTA",

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

    # posible inclusion: revision de renames de columnas para que sean consistentes con sus tipos
}


# esta funcion debe cambiarse a la existente
def get_object_lineage():
    return random.choice([True, False])

# construye y calcula el riesgo 
def _create_result(accion: str, objeto: Optional[str], columna: Optional[str], needs_lineage_check: bool) -> Dict[str, Any]:
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
            
    return {
        "accion": accion,
        "objeto": objeto,
        "columna": columna,
        "riesgo": riesgo_final
    }

# funcion principal para analizar todo el script 
def analizar_sql(path_sql: str):
    sql_text = Path(path_sql).read_text()
    statements = sqlparse.split(sql_text)

    # pasa por todas las sentencias
    resultados = []
    for stmt in statements:
        # se eliminan los comentarios de las sentencias para evitar que se interpreten comentarios como parte de la sentencia
        lines = stmt.strip().split('\n')
        cleaned_lines = [line for line in lines if not line.strip().startswith('--')]
        stmt_clean = '\n'.join(cleaned_lines).strip().upper()
        
        if not stmt_clean:
            continue

        # CREATE
        if re.match(r"^CREATE", stmt_clean):
            obj_type = ""
            if "TABLE" in stmt_clean: obj_type = "TABLE"
            elif "VIEW" in stmt_clean: obj_type = "VIEW"
            elif "SCHEMA" in stmt_clean: obj_type = "SCHEMA"
            elif "DATABASE" in stmt_clean: obj_type = "DATABASE"
            elif "WAREHOUSE" in stmt_clean: obj_type = "WAREHOUSE"
            elif "SHARE" in stmt_clean: obj_type = "SHARE"
            elif "TAG" in stmt_clean: obj_type = "TAG"
            elif "ACCESS_POLICY" in stmt_clean: obj_type = "ACCESS_POLICY"
            
            if obj_type:
                accion_base = f"CREATE_{obj_type}"
                needs_lineage_check = False
                
                if "OR REPLACE" in stmt_clean:
                    accion_base = f"CREATE_OR_REPLACE_{obj_type}"
                    needs_lineage_check = True
                elif "OR ALTER" in stmt_clean:
                    accion_base = f"CREATE_OR_ALTER_{obj_type}"
                    needs_lineage_check = True

                match = re.search(fr"{obj_type}\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result(accion_base, obj_name, None, needs_lineage_check))

        # ALTER 
        elif re.match(r"^ALTER", stmt_clean):
            if "TABLE" in stmt_clean:
                table_match = re.search(r"TABLE\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                tabla = table_match.group(1) if table_match else None
                
                if "ADD COLUMN" in stmt_clean:
                    col_match = re.search(r"ADD\s+COLUMN\s+([A-Z0-9_\"]+)", stmt_clean)
                    columna = col_match.group(1) if col_match else None
                    resultados.append(_create_result("ALTER_TABLE_ADD_COLUMN", tabla, columna, False))
                elif "DROP COLUMN" in stmt_clean:
                    col_match = re.search(r"DROP\s+COLUMN\s+([A-Z0-9_\"]+)", stmt_clean)
                    columna = col_match.group(1) if col_match else None
                    resultados.append(_create_result("ALTER_TABLE_DROP_COLUMN", tabla, columna, True))
                elif "ALTER COLUMN" in stmt_clean and "TYPE" in stmt_clean:
                    col_match = re.search(r"ALTER\s+COLUMN\s+([A-Z0-9_\"]+)", stmt_clean)
                    columna = col_match.group(1) if col_match else None
                    resultados.append(_create_result("ALTER_TABLE_MODIFY_COLUMN_TYPE", tabla, columna, True))
                else:
                    resultados.append(_create_result("ALTER_TABLE_NOT_COLUMNS", tabla, None, True))
            
            elif "VIEW" in stmt_clean:
                match = re.search(r"VIEW\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_VIEW", obj_name, None, True))
            elif "DATABASE" in stmt_clean:
                match = re.search(r"DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_DATABASE", obj_name, None, True))
            elif "SCHEMA" in stmt_clean:
                match = re.search(r"SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_SCHEMA", obj_name, None, True))
            elif "WAREHOUSE" in stmt_clean:
                match = re.search(r"WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_WAREHOUSE", obj_name, None, True))
            elif "SHARE" in stmt_clean:
                match = re.search(r"SHARE\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_SHARE", obj_name, None, True))
            elif "TAG" in stmt_clean:
                match = re.search(r"TAG\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_TAG", obj_name, None, True))
            elif "ACCESS_POLICY" in stmt_clean:
                match = re.search(r"ACCESS\s+POLICY\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_ACCESS_POLICY", obj_name, None, True))

        # DROP
        elif re.match(r"^DROP", stmt_clean):
            obj_type = ""
            if "TABLE" in stmt_clean: obj_type = "TABLE"
            elif "VIEW" in stmt_clean: obj_type = "VIEW"
            elif "SCHEMA" in stmt_clean: obj_type = "SCHEMA"
            elif "DATABASE" in stmt_clean: obj_type = "DATABASE"
            elif "WAREHOUSE" in stmt_clean: obj_type = "WAREHOUSE"
            elif "SHARE" in stmt_clean: obj_type = "SHARE"
            elif "TAG" in stmt_clean: obj_type = "TAG"
            elif "ACCESS_POLICY" in stmt_clean: obj_type = "ACCESS_POLICY"
            

            if obj_type:
                match = re.search(fr"{obj_type}\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result(f"DROP_{obj_type}", obj_name, None, True))

        # UNDROP
        elif re.match(r"^UNDROP", stmt_clean):
            obj_type = ""
            if "TABLE" in stmt_clean: obj_type = "TABLE"
            elif "SCHEMA" in stmt_clean: obj_type = "SCHEMA"
            elif "DATABASE" in stmt_clean: obj_type = "DATABASE"
            elif "TAG" in stmt_clean: obj_type = "TAG"

            if obj_type:
                match = re.search(fr"{obj_type}\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result(f"UNDROP_{obj_type}", obj_name, None, False))

        # TRUNCATE
        elif re.match(r"^TRUNCATE\s+TABLE", stmt_clean):
            match = re.search(r"TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            obj_name = match.group(1) if match else None
            resultados.append(_create_result("TRUNCATE_TABLE", obj_name, None, True))
        
        # INSERT DML
        elif re.match(r"^INSERT\s+INTO\s+([A-Z0-9_.\"]+)\s+VALUES", stmt_clean):
            tabla = re.findall(r"INSERT\s+INTO\s+([A-Z0-9_.\"]+)+VALUES\s", stmt_clean)
            resultados.append(_create_result(accion="INSERT_VALUES", objeto=tabla[0] if tabla else None, columna=None, needs_lineage_check=True))
        
        # MERGE DML
        elif re.match(r"^MERGE\s+INTO", stmt_clean):
            tabla = re.findall(r"MERGE\s+INTO\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append(_create_result(accion="MERGE_VALUES", objeto=tabla[0] if tabla else None, columna=None, needs_lineage_check=True))

        # DELETE DML
        elif re.match(r"^DELETE\s+FROM", stmt_clean):
            tabla = re.findall(r"DELETE\s+FROM\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append(_create_result(accion="DELETE_VALUES", objeto=tabla[0] if tabla else None, columna=None, needs_lineage_check=True))
        
        # GRANT PRIVILEGES
        elif re.match(r"^GRANT\s+", stmt_clean):
            match = re.search(r"GRANT\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)\s+TO\s+(ROLE|USER)\s+([A-Z0-9_]+)", stmt_clean)
            if match:
                resultados.append(_create_result(accion="GRANT_PRIVILEGE", objeto=match.group(2), columna=None, needs_lineage_check=True))

        # REVOKE PRIVILEGES
        elif re.match(r"^REVOKE\s+", stmt_clean):

            match = re.search(r"REVOKE\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)\s+FROM\s+(ROLE|USER)\s+([A-Z0-9_]+)", stmt_clean)
            if match:
                resultados.append(_create_result(accion="REVOKE_PRIVILEGE", objeto=match.group(2), columna=None, needs_lineage_check=False))

        
    hay_riesgo = any(r["riesgo"] in ["MEDIA", "ALTA"] for r in resultados)

    return hay_riesgo, resultados

def analizar_multiples_archivos(directorio: str = ".", patron: str = "*.sql", limite: int = 10) -> int:
    sql_files = []
    for root, dirs, files in os.walk(directorio):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))
    sql_files = [f for f in sql_files if not any(part.startswith('.') for part in Path(f).parts)]
    sql_files = sql_files[:limite]
    
    if not sql_files:
        print("No se encontraron archivos SQL para analizar")
        return 0
    
    total_risk = False
    results_summary = []
    
    for sql_file in sql_files:
        try:
            riesgo, resultados = analizar_sql(sql_file)
            
            if resultados:
                print(f'Archivo analizado: {sql_file}\n')
                results_summary.append({
                    'file': sql_file,
                    'risk': riesgo,
                    'count': len(resultados)
                })
                
                if riesgo:
                    total_risk = True
                
        except Exception as e:
            print(f" Error analizando {sql_file}: {str(e)}\n")
            return 1
    
    if total_risk:
        return 1
    else:
        return 0

if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = sys.argv[1]
        
        if os.path.isdir(path):
            exit_code = analizar_multiples_archivos(path)
            sys.exit(exit_code)
        else:
            riesgo, resultados = analizar_sql(path)
            for r in resultados:
                print(r)
            
            if riesgo:
                print("\nEl script debe ser revisado y no se puede aprobar automáticamente")
                sys.exit(1)
            else:
                print("\nEl script puede aprobarse automáticamente")
                sys.exit(0)
    else:
        exit_code = analizar_multiples_archivos(".")
        sys.exit(exit_code)

