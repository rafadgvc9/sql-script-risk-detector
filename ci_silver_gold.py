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

    # CONTEXT STATEMENTS
    "USE_DATABASE": "BAJA",
    "USE_SCHEMA": "BAJA",

    # posible inclusion: revision de renames de columnas para que sean consistentes con sus tipos
}

def parse_object_name(obj_name: str) -> Dict[str, Optional[str]]:
    """
    Analiza un nombre de objeto y determina si está completamente cualificado.
    Retorna un diccionario con database, schema, y object.
    Ejemplos:
    - "table_name" -> {"database": None, "schema": None, "object": "table_name"}
    - "schema.table_name" -> {"database": None, "schema": "schema", "object": "table_name"}
    - "database.schema.table_name" -> {"database": "database", "schema": "schema", "object": "table_name"}
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

# esta funcion debe cambiarse a la existente
def get_object_lineage():
    return random.choice([True, False])

# construye y calcula el riesgo 
def _create_result(
    accion: str, 
    objeto: Optional[str], 
    columna: Optional[str], 
    needs_lineage_check: bool, 
    object_info: Optional[Dict] = None
    ) -> Dict[str, Any]:
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
    
    return result

# funcion principal para analizar todo el script 
def analizar_sql(path_sql: str):
    sql_text = Path(path_sql).read_text()
    statements = sqlparse.split(sql_text)

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
        stmt_clean = '\n'.join(cleaned_lines).strip().upper()
        
        if not stmt_clean:
            continue
        
        # USE DATABASE
        if re.match(r"^USE\s+(DATABASE\s+)?([A-Z0-9_.\"]+)", stmt_clean):
            match = re.search(r"^USE\s+(?:DATABASE\s+)?([A-Z0-9_.\"]+)", stmt_clean)
            if match:
                db_name = match.group(1).strip('"').strip("'")
                current_context["database"] = db_name
                current_context["schema"] = None  
                resultados.append(_create_result("USE_DATABASE", db_name, None, False, 
                                                {"context": "database", "value": db_name}))
                continue
        
        # USE SCHEMA
        elif re.match(r"^USE\s+(SCHEMA\s+)?([A-Z0-9_.\"]+)", stmt_clean):
            match = re.search(r"^USE\s+(?:SCHEMA\s+)?([A-Z0-9_.\"]+)", stmt_clean)
            if match:
                db_part = match.group(1)
                schema_part = match.group(2) if match.group(2) else match.group(1)

                if db_part:
                    current_context["database"] = db_part.strip('"').strip("'")
                    current_context["schema"] = schema_part.strip('"').strip("'")
                else:
                    current_context["schema"] = schema_part.strip('"').strip("'")
                
                resultados.append(_create_result("USE_SCHEMA", 
                                                f"{db_part}.{schema_part}" if db_part else schema_part, 
                                                None, False, 
                                                {"context": "schema", 
                                                 "database": current_context["database"],
                                                 "schema": current_context["schema"]}))

                schema_name = match.group(1).strip('"').strip("'")
                current_context["schema"] = schema_name  
                resultados.append(_create_result("USE_SCHEMA", db_name, None, False, 
                                                {"context": "schema", "value": schema_name}))
                continue


        # CREATE
        elif re.match(r"^CREATE", stmt_clean):
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
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                
                resultados.append(_create_result(accion_base, obj_name, None, needs_lineage_check, object_info=obj_info))


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
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result("ALTER_VIEW", obj_name, None, True, object_info=obj_info))
            elif "DATABASE" in stmt_clean:
                match = re.search(r"DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result("ALTER_DATABASE", obj_name, None, True, object_info=obj_info))
            elif "SCHEMA" in stmt_clean:
                match = re.search(r"SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result("ALTER_SCHEMA", obj_name, None, True, object_info=obj_info))
            elif "WAREHOUSE" in stmt_clean:
                match = re.search(r"WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result("ALTER_WAREHOUSE", obj_name, None, True, object_info=obj_info))
            elif "SHARE" in stmt_clean:
                match = re.search(r"SHARE\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result("ALTER_SHARE", obj_name, None, True, object_info=obj_info))
            elif "TAG" in stmt_clean:
                match = re.search(r"TAG\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result("ALTER_TAG", obj_name, None, True, object_info=obj_info))
            elif "ACCESS_POLICY" in stmt_clean:
                match = re.search(r"ACCESS\s+POLICY\s+(?:IF\s+EXISTS\s+)?([A-Z0-9_.\"]+)", stmt_clean)
                obj_name = match.group(1) if match else None
                resultados.append(_create_result("ALTER_ACCESS_POLICY", obj_name, None, True, object_info=obj_info))

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
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result(f"DROP_{obj_type}", obj_name, None, True, object_info=obj_info))

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
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result(f"UNDROP_{obj_type}", obj_name, None, False, object_info=obj_info))

        # TRUNCATE
        elif re.match(r"^TRUNCATE\s+TABLE", stmt_clean):
            match = re.search(r"TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            obj_name = match.group(1) if match else None
            obj_info = parse_object_name(obj_name) if obj_name else None
            if obj_info:
                obj_info["current_context"] = current_context.copy()
            resultados.append(_create_result("TRUNCATE_TABLE", obj_name, None, True, object_info=obj_info))
        
        # INSERT DML
        elif re.match(r"^INSERT\s+INTO\s+([A-Z0-9_.\"]+)\s+VALUES", stmt_clean):
            tabla = re.findall(r"INSERT\s+INTO\s+([A-Z0-9_.\"]+)+VALUES\s", stmt_clean)
            obj_name = tabla[0] if tabla else None
            obj_info = parse_object_name(obj_name) if obj_name else None
            if obj_info:
                obj_info["current_context"] = current_context.copy()
            resultados.append(_create_result(accion="INSERT_VALUES", objeto=tabla[0] if tabla else None, columna=None, needs_lineage_check=True, object_info=obj_info))
        
        # MERGE DML
        elif re.match(r"^MERGE\s+INTO", stmt_clean):
            tabla = re.findall(r"MERGE\s+INTO\s+([A-Z0-9_.\"]+)", stmt_clean)
            obj_name = tabla[0] if tabla else None
            obj_info = parse_object_name(obj_name) if obj_name else None
            if obj_info:
                obj_info["current_context"] = current_context.copy()
            resultados.append(_create_result(accion="MERGE_VALUES", objeto=tabla[0] if tabla else None, columna=None, needs_lineage_check=True, object_info=obj_info))

        # DELETE DML
        elif re.match(r"^DELETE\s+FROM", stmt_clean):
            tabla = re.findall(r"DELETE\s+FROM\s+([A-Z0-9_.\"]+)", stmt_clean)
            obj_name = tabla[0] if tabla else None
            obj_info = parse_object_name(obj_name) if obj_name else None
            if obj_info:
                obj_info["current_context"] = current_context.copy()
            resultados.append(_create_result(accion="DELETE_VALUES", objeto=tabla[0] if tabla else None, columna=None, needs_lineage_check=True, object_info=obj_info))
        
        # GRANT PRIVILEGES
        elif re.match(r"^GRANT\s+", stmt_clean):
            match = re.search(r"GRANT\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)\s+TO\s+(ROLE|USER)\s+([A-Z0-9_]+)", stmt_clean)
            if match:
                obj_name = match.group(2)
                obj_info = parse_object_name(obj_name) if obj_name else None
                if obj_info:
                    obj_info["current_context"] = current_context.copy()
                resultados.append(_create_result(accion="GRANT_PRIVILEGE", objeto=obj_name, columna=None, needs_lineage_check=True, object_info=obj_info))

        # REVOKE PRIVILEGES
        elif re.match(r"^REVOKE\s+", stmt_clean):

            match = re.search(r"REVOKE\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)\s+FROM\s+(ROLE|USER)\s+([A-Z0-9_]+)", stmt_clean)
            if match:
                resultados.append(_create_result(accion="REVOKE_PRIVILEGE", objeto=match.group(2), columna=None, needs_lineage_check=False, object_info=obj_info))

        
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
    risky_files = []
    
    for sql_file in sql_files:
        try:
            riesgo, resultados = analizar_sql(sql_file)
            
            if resultados:
                
                if riesgo:
                    total_risk = True
                    risky_sentences = [r for r in resultados if r["riesgo"] in ["MEDIA", "ALTA"]]
                    risky_files.append({
                    'file': sql_file,
                    'sentences': risky_sentences
                })
               
           
                
        except Exception as e:
            print(f" Error analizando {sql_file}: {str(e)}\n")
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
                    ctx = obj_info.get('current_context', {})
                    if ctx.get('database') or ctx.get('schema'):
                        print(f"   Contexto activo -> Database: {ctx.get('database', 'N/A')}, Schema: {ctx.get('schema', 'N/A')}")
                
        return 1
    else:
        print("   No se detectaron operaciones de alto riesgo")
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

