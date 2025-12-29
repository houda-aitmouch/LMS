from app import execute_query

def get_chart_data(metric, dimension, filters):
    """
    Construire dynamiquement la requête SQL pour Delta Lake
    AVEC paramètres formatés correctement pour Databricks
    """
    from config import execute_query_safe
    
    # Mapping dimension → table et colonnes
    dimension_mapping = {
        'domain': {
            'table': 'default.dim_domain',
            'column': 'domain_name',
            'key': 'domain_key'
        },
        'level': {
            'table': 'default.dim_course', 
            'column': 'level',
            'key': 'course_key'
        },
        'category': {
            'table': 'default.dim_course',
            'column': 'category',
            'key': 'course_key'
        },
        'lms_source': {
            'table': 'default.dim_lms',
            'column': 'lms_source',
            'key': 'lms_key'
        },
        'year': {
            'table': 'default.dim_date',
            'column': 'year',
            'key': 'date_key'
        },
        'month': {
            'table': 'default.dim_date',
            'column': 'month',
            'key': 'date_key'
        }
    }
    
    # VÉRIFICATION que la dimension existe
    if dimension not in dimension_mapping:
        raise ValueError(f"Dimension non supportée: {dimension}")
    
    dim_info = dimension_mapping[dimension]
    
    # VALIDATION de la métrique
    valid_metrics = ['completion_rate_percent', 'score_obtained_percent', 'engagement_ratio', 
                    'duration_spent_hours', 'certified', 'dropout_flag','score_per_hour',
                    'time_to_complete_days', 'completed_flag', 'is_high_performer', 'is_at_risk']
    if metric not in valid_metrics:
        raise ValueError(f"Métrique non supportée: {metric}")
    
    # Déterminer l'agrégation selon la métrique
    if metric in ['certified', 'dropout_flag']:
        agg_function = f"AVG(CAST(f.{metric} AS DOUBLE)) * 100"
    else:
        agg_function = f"AVG(f.{metric})"
    
    # Construction SÉCURISÉE de la requête AVEC valeurs directes
    base_query = f"""
        SELECT 
            d.{dim_info['column']} as dimension_value,
            {agg_function} as metric_value,
            COUNT(*) as count
        FROM default.fact_learning f
        JOIN {dim_info['table']} d ON f.{dim_info['key']} = d.{dim_info['key']}
    """
    
    # Construction des clauses WHERE avec valeurs DIRECTES
    where_clauses = []
    
    if filters.get('lms_source'):
        lms_value = filters['lms_source'].replace("'", "''")  # Échapper les quotes
        where_clauses.append(f"""
            EXISTS (
                SELECT 1 FROM default.dim_lms l 
                WHERE f.lms_key = l.lms_key 
                AND l.lms_source = '{lms_value}'
            )
        """)
    
    if filters.get('year'):
        year_value = filters['year']
        where_clauses.append(f"""
            EXISTS (
                SELECT 1 FROM default.dim_date dt 
                WHERE f.date_key = dt.date_key 
                AND dt.year = {year_value}
            )
        """)
    
    if filters.get('domain'):
        domain_value = filters['domain'].replace("'", "''")  # Échapper les quotes
        where_clauses.append(f"""
            EXISTS (
                SELECT 1 FROM default.dim_domain dom 
                WHERE f.domain_key = dom.domain_key 
                AND dom.domain_name = '{domain_value}'
            )
        """)
    
    # Ajouter les WHERE si nécessaire
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    
    # Finaliser la requête
    base_query += f"""
        GROUP BY d.{dim_info['column']}
        ORDER BY metric_value DESC
        LIMIT 50
    """
    
    print(f"🔍 Requête générée: {base_query[:200]}...")
    
    # Exécuter la requête SANS paramètres (valeurs directes)
    results = execute_query_safe(base_query)
    
    # Formater les résultats
    formatted_results = []
    for row in results:
        try:
            formatted_results.append({
                'name': str(row['dimension_value']),
                'value': round(float(row['metric_value'] or 0), 2),
                'count': int(row['count'])
            })
        except (ValueError, TypeError) as e:
            print(f"⚠️ Erreur formatage ligne: {row}, erreur: {e}")
            continue
    
    print(f"✅ Données formatées: {len(formatted_results)} lignes")
    return formatted_results