from flask import Flask, jsonify
from flask_cors import CORS
from config import DatabricksConfig, test_connection

app = Flask(__name__)
CORS(app)

# Déplacer execute_query AVANT les imports des routes
def execute_query(query, params=None):
    """Exécuter une requête SQL sur Delta Lake"""
    conn = DatabricksConfig.get_connection()
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Récupérer les résultats
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        
        # Convertir en liste de dictionnaires
        data = [dict(zip(columns, row)) for row in results]
        
        return data
    
    except Exception as e:
        print(f"❌ Erreur SQL: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# Tester la connexion au démarrage
@app.before_request
def startup():
    if not hasattr(app, 'databricks_checked'):
        print("🔄 Test de connexion à Databricks...")
        if test_connection():
            print("✅ Application prête !")
            app.databricks_checked = True
        else:
            print("❌ Problème de connexion à Databricks")

# Route santé
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "message": "Flask API is running"})

# MAINTENANT importer les routes
from routes.filters import bp as filters_bp
from routes.charts import bp as charts_bp

app.register_blueprint(filters_bp)
app.register_blueprint(charts_bp)

if __name__ == '__main__':
    app.run(debug=True, port=5000)