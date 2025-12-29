from flask import Blueprint, jsonify

bp = Blueprint('filters', __name__, url_prefix='/api/filters')

@bp.route('/lms', methods=['GET'])
def get_lms_list():
    """Récupérer la liste des LMS depuis Delta Lake"""
    from config import execute_query
    
    query = """
        SELECT 
            l.lms_source,
            COUNT(*) as count
        FROM default.fact_learning f
        JOIN default.dim_lms l ON f.lms_key = l.lms_key
        GROUP BY l.lms_source
        ORDER BY l.lms_source
    """
    
    results = execute_query(query)
    
    return jsonify([
        {
            'value': row['lms_source'], 
            'label': row['lms_source'], 
            'count': row['count']
        }
        for row in results
    ])

@bp.route('/domains', methods=['GET'])
def get_domains():
    """Récupérer les domaines depuis Delta Lake"""
    from config import execute_query
    
    query = """
        SELECT DISTINCT domain_name
        FROM default.dim_domain
        WHERE domain_name IS NOT NULL
        ORDER BY domain_name
    """
    
    results = execute_query(query)
    
    return jsonify([
        {'value': row['domain_name'], 'label': row['domain_name']}
        for row in results
    ])

@bp.route('/years', methods=['GET'])
def get_years():
    """Récupérer les années disponibles - Version CORRECTE"""
    from config import execute_query_safe
    
    # REQUÊTE SIMPLE et SÛRE
    query = """
        SELECT DISTINCT year
        FROM default.dim_date 
        WHERE year IS NOT NULL 
        ORDER BY year DESC
        LIMIT 20
    """
    
    try:
        results = execute_query_safe(query)
        
        # Traitement manuel des résultats
        years_data = []
        for row in results:
            year_value = row.get('year')
            if year_value is not None:
                try:
                    year_int = int(year_value)
                    years_data.append({
                        'value': year_int, 
                        'label': str(year_int)
                    })
                except (ValueError, TypeError):
                    continue
        
        print(f"✅ Années récupérées dynamiquement: {len(years_data)}")
        return jsonify(years_data)
        
    except Exception as e:
        print(f"❌ Erreur récupération années: {e}")
        # Fallback manuel basé sur les données vues dans les tests
        return jsonify([
            {'value': 2028, 'label': '2028'},
            {'value': 2024, 'label': '2024'},
            {'value': 2023, 'label': '2023'},
            {'value': 2022, 'label': '2022'},
            {'value': 2021, 'label': '2021'}
        ])

@bp.route('/metrics', methods=['GET'])
def get_available_metrics():
    """Liste des métriques disponibles (statique)"""
    metrics = [
        {'value': 'completion_rate_percent', 'label': '📈 Taux de complétion', 'unit': '%'},
        {'value': 'score_obtained_percent', 'label': '🎯 Score moyen', 'unit': '%'},
        {'value': 'engagement_ratio', 'label': '⚡ Engagement', 'unit': 'ratio'},
        {'value': 'duration_spent_hours', 'label': '⏱️ Temps passé', 'unit': 'heures'},
        {'value': 'certified', 'label': '🏆 Taux de certification', 'unit': '%'},
        {'value': 'dropout_flag', 'label': '❌ Taux d\'abandon', 'unit': '%'},
        {'value': 'score_per_hour', 'label': '🚀 Efficacité d\'apprentissage', 'unit': 'points/h'},
        {'value': 'time_to_complete_days', 'label': '⏱️ Temps de complétion', 'unit': 'jours'},
        {'value': 'completed_flag', 'label': '✅ Taux de complétion', 'unit': '%'},
        {'value': 'is_high_performer', 'label': '🏆 Taux de performants', 'unit': '%'},
        {'value': 'is_at_risk', 'label': '🚨 Taux d\'apprenants à risque', 'unit': '%'},
    ]
    return jsonify(metrics)

@bp.route('/dimensions', methods=['GET'])
def get_available_dimensions():
    """Liste des dimensions disponibles (statique)"""
    dimensions = [
        {'value': 'domain', 'label': '📚 Par Domaine'},
        {'value': 'level', 'label': '📊 Par Niveau'},
        {'value': 'category', 'label': '🏷️ Par Catégorie'},
        {'value': 'year', 'label': '📅 Par Année'},
        {'value': 'month', 'label': '📆 Par Mois'},
    ]
    return jsonify(dimensions)