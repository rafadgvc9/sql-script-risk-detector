from functions_library import analizar_sql
from typing import List, Dict
from pathlib import Path
import os
import sys

def analizar_multiples_archivos(archivos_sql: List[str] = None, 
                                template_vars: Dict[str, str] = None) -> int:
    if archivos_sql is None:
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
