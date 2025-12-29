from flask import Blueprint, request, jsonify

bp = Blueprint('charts', __name__, url_prefix='/api/charts')

@bp.route('/generate', methods=['POST'])
def generate():
    """
    Générer un graphique depuis Delta Lake
    """
    try:
        from services.data_service import get_chart_data
        
        data = request.json
        
        # Récupérer les données depuis Delta Lake
        chart_data = get_chart_data(
            metric=data['metric'],
            dimension=data['dimension'],
            filters=data['filters']
        )
        
        return jsonify({
            'success': True,
            'data': chart_data,
            'config': {
                'metric': data['metric'],
                'dimension': data['dimension'],
                'chart_type': data.get('chart_type', 'bar')
            }
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/multi-charts', methods=['POST'])
def multi_charts():
    """Générer plusieurs graphiques en parallèle"""
    try:
        from services.data_service import get_chart_data
        
        data = request.json
        
        results = []
        for chart_config in data['charts']:
            chart_data = get_chart_data(
                metric=chart_config['metric'],
                dimension=chart_config['dimension'],
                filters=data['filters']
            )
            
            results.append({
                'id': chart_config['id'],
                'data': chart_data,
                'config': chart_config
            })
        
        return jsonify({
            'success': True,
            'charts': results
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/stats', methods=['POST'])
def get_stats():
    """Récupérer les statistiques globales pour le contexte sélectionné"""
    try:
        from config import execute_query
        
        data = request.json
        filters = data['filters']
        
        # Construire les WHERE clauses
        where_clauses = []
        if filters.get('lms_source'):
            where_clauses.append(f"l.lms_source = '{filters['lms_source']}'")
        if filters.get('year'):
            where_clauses.append(f"d.year = {filters['year']}")
        if filters.get('domain'):
            where_clauses.append(f"dom.domain_name = '{filters['domain']}'")
        
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
            SELECT 
                COUNT(*) as total_enrollments,
                AVG(f.completion_rate_percent) as avg_completion,
                AVG(f.score_obtained_percent) as avg_score,
                AVG(f.engagement_ratio) as avg_engagement,
                SUM(CAST(f.certified AS INT)) as total_certified,
                SUM(CAST(f.dropout_flag AS INT)) as total_dropouts
            FROM default.fact_learning f
            JOIN default.dim_lms l ON f.lms_key = l.lms_key
            JOIN default.dim_date d ON f.date_key = d.date_key
            JOIN default.dim_domain dom ON f.domain_key = dom.domain_key
            {where_sql}
        """
        
        result = execute_query(query)[0]
        
        return jsonify({
            'success': True,
            'stats': {
                'total_enrollments': result['total_enrollments'],
                'avg_completion': round(result['avg_completion'], 2),
                'avg_score': round(result['avg_score'], 2),
                'avg_engagement': round(result['avg_engagement'], 2),
                'certification_rate': round((result['total_certified'] / result['total_enrollments']) * 100, 2),
                'dropout_rate': round((result['total_dropouts'] / result['total_enrollments']) * 100, 2)
            }
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500