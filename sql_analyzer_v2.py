import sqlparse
import random
import re
from pathlib import Path

# definicion de riesgos para cada accion
RIESGO = {

    # DDL TABLAS 
    "CREATE_TABLE": "BAJA",                 
    "DROP_TABLE": "ALTA",                   
    "CREATE_OR_REPLACE_TABLE": "ALTA",      
    "CREATE_OR_ALTER_TABLE": "MEDIA",       
    "UNDROP_TABLE": "BAJA",
    "TRUNCATE_TABLE": "ALTA",
    "ALTER_TABLE_NOT_COLUMNS": "ALTA",     

    # DML
    "INSERT_VALUES": "MEDIA",                      
    "DELETE_VALUES": "ALTA",                       
    "MERGE_VALUES": "MEDIA",                       

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
    "CREATE_OR_ALTER_VIEW": "MEDIA",        
    "ALTER_VIEW": "MEDIA",                  
    "CREATE_OR_REPLACE_VIEW": "ALTA",
    "DROP_VIEW": "ALTA",                    

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

    # TODO: rename que sean consistentes con sus tipos


    # naming convention bien?
    # alterar columna de tipo?
}


def get_object_lineage():
    return random.choice([True, False])

def analizar_sql(path_sql: str):
    sql_text = Path(path_sql).read_text()
    statements = sqlparse.split(sql_text)

    resultados = []
    for stmt in statements:
        stmt_clean = stmt.strip().upper()

        #-----------------------------------------------------------------------------
        #---------------------------------------TABLE---------------------------------
        #-----------------------------------------------------------------------------

        # CREATE OR REPLACE TABLE
        if re.match(r"^CREATE\s+OR\s+REPLACE\s+TABLE", stmt_clean):
            tabla = re.findall(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_REPLACE_TABLE",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_REPLACE_TABLE"] if is_used else "BAJA"
            })
        
        # CREATE OR ALTER TABLE  
        elif re.match(r"^CREATE\s+OR\s+ALTER\s+TABLE", stmt_clean):
            tabla = re.findall(r"CREATE\s+OR\s+ALTER\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_ALTER_TABLE",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_ALTER_TABLE"] if is_used else "BAJA"
            })
        
        # CREATE TABLE
        elif re.match(r"^CREATE\s+TABLE", stmt_clean):
            tabla = re.findall(r"CREATE\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_TABLE",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_TABLE"]
            })

        # UNDROP TABLE
        elif re.match(r"^UNDROP\s+TABLE", stmt_clean):
            tabla = re.findall(r"UNDROP\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "UNDROP_TABLE",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["UNDROP_TABLE"]
            })
        
        # DROP TABLE
        elif re.match(r"^DROP\s+TABLE", stmt_clean):
            tabla = re.findall(r"DROP\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "DROP_TABLE",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["DROP_TABLE"] if is_used else "BAJA"
            })

        # TRUNCATE TABLE
        elif re.match(r"^TRUNCATE\s+TABLE", stmt_clean):
            tabla = re.findall(r"TRUNCATE\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "TRUNCATE_TABLE",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["TRUNCATE_TABLE"]
            })

        #-----------------------------------------------------------------------------
        #---------------------------------------VIEW----------------------------------
        #-----------------------------------------------------------------------------

        # CREATE OR REPLACE VIEW
        elif re.match(r"^CREATE\s+OR\s+REPLACE\s+VIEW", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_REPLACE_VIEW",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_REPLACE_VIEW"] if is_used else "BAJA"
            })

        # CREATE OR ALTER VIEW
        elif re.match(r"^CREATE\s+OR\s+ALTER\s+VIEW", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+ALTER\s+VIEW\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_ALTER_VIEW",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_ALTER_VIEW"] if is_used else "BAJA"
            })

        # CREATE VIEW
        elif re.match(r"^CREATE\s+VIEW", stmt_clean):
            vista = re.findall(r"CREATE\s+VIEW\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_VIEW",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_VIEW"]
            })

        # ALTER VIEW
        elif re.match(r"^ALTER\s+VIEW", stmt_clean):
            vista = re.findall(r"ALTER\s+VIEW\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "ALTER_VIEW",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_VIEW"] if is_used else "BAJA"
            })

        
        
        # DROP VIEW
        elif re.match(r"^DROP\s+VIEW", stmt_clean):
            vista = re.findall(r"DROP\s+VIEW\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "DROP_VIEW",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["DROP_VIEW"] if is_used else "BAJA"
            })

        #-----------------------------------------------------------------------------
        #---------------------------------------DATABASE------------------------------
        #-----------------------------------------------------------------------------

        # CREATE OR REPLACE DATABASE
        elif re.match(r"^CREATE\s+OR\s+REPLACE\s+DATABASE", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+REPLACE\s+DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_REPLACE_DATABASE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_REPLACE_DATABASE"] if is_used else "BAJA"
            })

        # CREATE OR ALTER DATABASE
        elif re.match(r"^CREATE\s+OR\s+ALTER\s+DATABASE", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+ALTER\s+DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_ALTER_DATABASE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_ALTER_DATABASE"] if is_used else "BAJA"
            })

        # CREATE DATABASE
        elif re.match(r"^CREATE\s+DATABASE", stmt_clean):
            vista = re.findall(r"CREATE\s+DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_DATABASE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_DATABASE"]
            })
        
        # ALTER DATABASE
        elif re.match(r"^ALTER\s+DATABASE", stmt_clean):
            vista = re.findall(r"ALTER\s+DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "ALTER_DATABASE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_DATABASE"] if is_used else "BAJA"
            })

        # UNDROP DATABASE
        elif re.match(r"^UNDROP\s+DATABASE", stmt_clean):
            vista = re.findall(r"UNDROP\s+DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "UNDROP_DATABASE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["UNDROP_DATABASE"]
            })

        # DROP DATABASE
        elif re.match(r"^DROP\s+DATABASE", stmt_clean):
            vista = re.findall(r"DROP\s+DATABASE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "DROP_DATABASE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["DROP_DATABASE"] if is_used else "BAJA"
            })

        #-----------------------------------------------------------------------------
        #---------------------------------------WAREHOUSE-----------------------------
        #-----------------------------------------------------------------------------

        # CREATE OR ALTER WAREHOUSE
        elif re.match(r"^CREATE\s+OR\s+ALTER\s+WAREHOUSE", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+ALTER\s+WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_ALTER_WAREHOUSE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_ALTER_WAREHOUSE"] if is_used else "BAJA"
            })

        # CREATE WAREHOUSE
        elif re.match(r"^CREATE\s+WAREHOUSE", stmt_clean):
            vista = re.findall(r"CREATE\s+WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_WAREHOUSE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_WAREHOUSE"]
            })
        
        # ALTER WAREHOUSE
        elif re.match(r"^ALTER\s+WAREHOUSE", stmt_clean):
            vista = re.findall(r"ALTER\s+WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "ALTER_WAREHOUSE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_WAREHOUSE"] if is_used else "BAJA"
            })

        # DROP WAREHOUSE
        elif re.match(r"^DROP\s+WAREHOUSE", stmt_clean):
            vista = re.findall(r"DROP\s+WAREHOUSE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "DROP_WAREHOUSE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["DROP_WAREHOUSE"]
            })

        
        #-----------------------------------------------------------------------------
        #---------------------------------------SCHEMA--------------------------------
        #-----------------------------------------------------------------------------

        # CREATE OR REPLACE SCHEMA
        elif re.match(r"^CREATE\s+OR\s+REPLACE\s+SCHEMA", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+REPLACE\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_REPLACE_SCHEMA",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_REPLACE_SCHEMA"] if is_used else "BAJA"
            })

        # CREATE OR ALTER SCHEMA
        elif re.match(r"^CREATE\s+OR\s+ALTER\s+SCHEMA", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+ALTER\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_ALTER_SCHEMA",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_ALTER_SCHEMA"] if is_used else "BAJA"
            })

        # CREATE SCHEMA
        elif re.match(r"^CREATE\s+SCHEMA", stmt_clean):
            vista = re.findall(r"CREATE\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_SCHEMA",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_SCHEMA"]
            })
        
        # ALTER SCHEMA
        elif re.match(r"^ALTER\s+SCHEMA", stmt_clean):
            vista = re.findall(r"ALTER\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "ALTER_SCHEMA",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_SCHEMA"] if is_used else "BAJA"
            })

        # UNDROP SCHEMA
        elif re.match(r"^UNDROP\s+SCHEMA", stmt_clean):
            vista = re.findall(r"UNDROP\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "UNDROP_SCHEMA",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["UNDROP_SCHEMA"]
            })

        # DROP SCHEMA
        elif re.match(r"^DROP\s+SCHEMA", stmt_clean):
            vista = re.findall(r"DROP\s+SCHEMA\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "DROP_SCHEMA",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["DROP_SCHEMA"] if is_used else "BAJA"
            })

        #-----------------------------------------------------------------------------
        #---------------------------------------SHARE--------------------------------
        #-----------------------------------------------------------------------------

        # CREATE OR ALTER SHARE
        elif re.match(r"^CREATE\s+OR\s+ALTER\s+SHARE", stmt_clean):
            vista = re.findall(r"CREATE\s+OR\s+ALTER\s+SHARE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "CREATE_OR_ALTER_SHARE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_OR_ALTER_SHARE"] if is_used else "BAJA"
            })

        # CREATE SHARE
        elif re.match(r"^CREATE\s+SHARE", stmt_clean):
            vista = re.findall(r"CREATE\s+SHARE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_SHARE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_SHARE"]
            })
        
        # ALTER SHARE
        elif re.match(r"^ALTER\s+SHARE", stmt_clean):
            vista = re.findall(r"ALTER\s+SHARE\s+([A-Z0-9_.\"]+)", stmt_clean)
            is_used = get_object_lineage()
            resultados.append({
                "accion": "ALTER_SHARE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_SHARE"] if is_used else "BAJA"
            })

        # DROP SHARE
        elif re.match(r"^DROP\s+SHARE", stmt_clean):
            vista = re.findall(r"DROP\s+SHARE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "DROP_SHARE",
                "objeto": vista[0] if vista else None,
                "columna": None,
                "riesgo": RIESGO["DROP_SHARE"]
            })
        
        # ALTER TABLE ADD COLUMN
        elif re.match(r"^ALTER\s+TABLE.*ADD\s+COLUMN", stmt_clean):
            match = re.search(r"ALTER\s+TABLE\s+([A-Z0-9_.\"]+).*ADD\s+COLUMN\s+([A-Z0-9_\".]+)", stmt_clean)
            if match:
                resultados.append({
                    "accion": "ALTER_TABLE_ADD_COLUMN",
                    "objeto": match.group(1),
                    "columna": match.group(2),
                    "riesgo": RIESGO["ALTER_TABLE_ADD_COLUMN"]
                })

        # ALTER TABLE DROP COLUMN
        elif re.match(r"^ALTER\s+TABLE.*DROP\s+COLUMN", stmt_clean):
            match = re.search(r"ALTER\s+TABLE\s+([A-Z0-9_.\"]+).*DROP\s+COLUMN\s+([A-Z0-9_\".]+)", stmt_clean)
            is_used = get_object_lineage()
            if match:
                resultados.append({
                    "accion": "ALTER_TABLE_DROP_COLUMN",
                    "objeto": match.group(1),
                    "columna": match.group(2),
                    "riesgo": RIESGO["ALTER_TABLE_DROP_COLUMN"] if is_used else "BAJA"
                })

        # ALTER TABLE MODIFY COLUMN TYPE
        elif re.match(r"^ALTER\s+TABLE.*ALTER\s+COLUMN.*TYPE", stmt_clean):
            match = re.search(r"ALTER\s+TABLE\s+([A-Z0-9_.\"]+).*ALTER\s+COLUMN\s+([A-Z0-9_.\"]+)\s+TYPE\s+([A-Z0-9_()]+)", stmt_clean)
            is_used = get_object_lineage()
            if match:
                resultados.append({
                    "accion": "ALTER_TABLE_DROP_COLUMN",
                    "objeto": match.group(1),
                    "columna": match.group(2),
                    "riesgo": RIESGO["ALTER_TABLE_DROP_COLUMN"] if is_used else "BAJA"
                })
        
        # ALTER TABLE
        elif re.match(r"^ALTER\s+TABLE", stmt_clean):
            tabla = re.findall(r"ALTER\s+TABLE\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "ALTER_TABLE_NOT_COLUMNS",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_TABLE_NOT_COLUMNS"] 
            })
        
        #-----------------------------------------------------------------------------
        #---------------------------------------DML-----------------------------------
        #-----------------------------------------------------------------------------

        
        # INSERT DML
        elif re.match(r"^INSERT\s+INTO\s+([A-Z0-9_.\"]+)\s+VALUES", stmt_clean):
            tabla = re.findall(r"INSERT\s+INTO\s+([A-Z0-9_.\"]+)+VALUES\s", stmt_clean)
            resultados.append({
                "accion": "INSERT_VALUES",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["INSERT_VALUES"] 
            })
        
        # MERGE DML
        elif re.match(r"^MERGE\s+INTO", stmt_clean):
            tabla = re.findall(r"MERGE\s+INTO\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "MERGE_VALUES",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["MERGE_VALUES"] 
            })

        # DELETE DML
        elif re.match(r"^DELETE\s+FROM", stmt_clean):
            tabla = re.findall(r"DELETE\s+FROM\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "DELETE_VALUES",
                "objeto": tabla[0] if tabla else None,
                "columna": None,
                "riesgo": RIESGO["DELETE_VALUES"] 
            })
        
        # GRANT PRIVILEGES
        if re.match(r"^GRANT\s+", stmt_clean):
            match = re.search(r"GRANT\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)\s+TO\s+(ROLE|USER)\s+([A-Z0-9_]+)", stmt_clean)
            if match:
                resultados.append({
                    "accion": "GRANT_PRIVILEGE",
                    "objeto": match.group(2),
                    "columna": None,
                    "riesgo": RIESGO["GRANT_PRIVILEGE"]
                })

        # REVOKE PRIVILEGES
        elif re.match(r"^REVOKE\s+", stmt_clean):

            match = re.search(r"REVOKE\s+([A-Z_,\s]+)\s+ON\s+[A-Z_]+\s+([A-Z0-9_.\"]+)\s+FROM\s+(ROLE|USER)\s+([A-Z0-9_]+)", stmt_clean)
            if match:
                resultados.append({
                    "accion": "REVOKE_PRIVILEGE",
                    "objeto": match.group(2),
                    "columna": None,
                    "riesgo": RIESGO["REVOKE_PRIVILEGE"]
                })

        # CREATE (OR REPLACE) ROW ACCESS POLICY
        elif re.match(r"^CREATE(\s+OR\s+REPLACE)?\s+ROW\s+ACCESS\s+POLICY", stmt_clean):
            policy = re.findall(r"ROW\s+ACCESS\s+POLICY\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "CREATE_ACCESS_POLICY",
                "objeto": policy[0] if policy else None,
                "columna": None,
                "riesgo": RIESGO["CREATE_ACCESS_POLICY"]
            })

        # ALTER ROW ACCESS POLICY
        elif re.match(r"^ALTER\s+ROW\s+ACCESS\s+POLICY", stmt_clean):
            policy = re.findall(r"ROW\s+ACCESS\s+POLICY\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "ALTER_ACCESS_POLICY",
                "objeto": policy[0] if policy else None,
                "columna": None,
                "riesgo": RIESGO["ALTER_ACCESS_POLICY"]
            })

        # DROP ROW ACCESS POLICY
        elif re.match(r"^DROP\s+ROW\s+ACCESS\s+POLICY", stmt_clean):
            policy = re.findall(r"ROW\s+ACCESS\s+POLICY\s+([A-Z0-9_.\"]+)", stmt_clean)
            resultados.append({
                "accion": "DROP_ACCESS_POLICY",
                "objeto": policy[0] if policy else None,
                "columna": None,
                "riesgo": RIESGO["DROP_ACCESS_POLICY"]
            })
        
    hay_riesgo = any(r["riesgo"] in ["MEDIA", "ALTA"] for r in resultados)

    return hay_riesgo, resultados


if __name__ == "__main__":
    path = "script.sql" 
    resultados, riesgo = analizar_sql(path)
    for r in resultados:
        print(r)
    
    if riesgo:
        print("El script debe ser revisado y no se puede aprobar automáticamente")
    else: 
        print("El script puede aprobarse automáticamente")

