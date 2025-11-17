import re
from typing import List, Dict, Optional, Set

def extract_procedure_variables(proc_body: str) -> Set[str]:
    """
    Extrae todas las variables declaradas en un procedimiento.
    
    Args:
        proc_body: El cuerpo del procedimiento SQL
    
    Returns:
        Set[str]: Conjunto de nombres de variables declaradas
    """
    # Busca declaraciones de variables
    declare_pattern = re.compile(r'\b(?:DECLARE|LET)\s+([A-Za-z_][A-Za-z0-9_]*)\s', re.IGNORECASE)
    # Busca asignaciones de variables
    assign_pattern = re.compile(r'\b([A-Za-z_][A-Za-z0-9_]*)\s*:=\s*', re.IGNORECASE)
    
    variables = set()
    
    # Encuentra todas las variables declaradas
    for match in declare_pattern.finditer(proc_body):
        variables.add(match.group(1).lower())
        
    # Encuentra todas las variables asignadas
    for match in assign_pattern.finditer(proc_body):
        variables.add(match.group(1).lower())
        
    return variables

def is_procedure_variable(name: str, proc_variables: Set[str]) -> bool:
    """
    Determina si un nombre es una variable del procedimiento.
    
    Args:
        name: Nombre de la variable a comprobar
        proc_variables: Conjunto de variables del procedimiento
    
    Returns:
        bool: True si es una variable del procedimiento
    """
    return name.lower() in proc_variables

def find_identifier_variables(sql: str) -> List[str]:
    """
    Encuentra todas las variables usadas en funciones identifier().
    
    Args:
        sql: Código SQL a analizar
    
    Returns:
        List[str]: Lista de nombres de variables encontradas
    """
    pattern = re.compile(r'identifier\s*\(\s*:([A-Za-z_][A-Za-z0-9_]*)\s*\)', re.IGNORECASE)
    return [m.group(1) for m in pattern.finditer(sql)]

def process_identifier_calls(sql: str, proc_variables: Set[str], template_vars: Optional[Dict[str, str]] = None) -> str:
    """
    Procesa las llamadas a identifier() teniendo en cuenta variables de procedimiento y template.
    
    Args:
        sql: Código SQL a procesar
        proc_variables: Variables declaradas en el procedimiento
        template_vars: Variables de template disponibles
    
    Returns:
        str: SQL con las variables procesadas
    """
    if template_vars is None:
        template_vars = {}

    def replace_identifier(match):
        var_name = match.group(1)
        # Si es una variable del procedimiento, la dejamos como está
        if is_procedure_variable(var_name, proc_variables):
            return match.group(0)
        # Si es una variable de template, la reemplazamos
        var_value = next((v for k, v in template_vars.items() if k.lower() == var_name.lower()), None)
        if var_value is not None:
            return f"identifier('{var_value}')"
        # Si no es ninguna de las anteriores, la marcamos como desconocida
        return f"identifier('VAR_{var_name.upper()}')"

    pattern = re.compile(r'identifier\s*\(\s*:([A-Za-z_][A-Za-z0-9_]*)\s*\)', re.IGNORECASE)
    return pattern.sub(replace_identifier, sql)