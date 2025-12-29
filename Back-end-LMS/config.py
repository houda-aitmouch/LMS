import os
import sys
from dotenv import load_dotenv

print("🚀 Initialisation de la configuration Databricks...")

# Charger .env
env_path = os.path.join(os.getcwd(), '.env')
if os.path.exists(env_path):
    print("✅ Fichier .env trouvé!")
    load_dotenv(env_path)
else:
    print("❌ Fichier .env NON TROUVÉ!")

# Import sécurisé du connecteur Databricks
try:
    from databricks import sql
    print("✅ Connecteur Databricks importé avec succès")
except ImportError as e:
    print(f"❌ ERREUR: Impossible d'importer le connecteur Databricks: {e}")
    print("💡 Solution: Exécutez: pip install databricks-sql-connector")
    sys.exit(1)

class DatabricksConfig:
    """Configuration pour se connecter à Databricks"""
    
    SERVER_HOSTNAME = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
    ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    @staticmethod
    def validate_config():
        """Valider que la configuration est complète"""
        missing = []
        if not DatabricksConfig.SERVER_HOSTNAME:
            missing.append("DATABRICKS_SERVER_HOSTNAME")
        if not DatabricksConfig.HTTP_PATH:
            missing.append("DATABRICKS_HTTP_PATH") 
        if not DatabricksConfig.ACCESS_TOKEN:
            missing.append("DATABRICKS_ACCESS_TOKEN")
        
        if missing:
            error_msg = f"❌ Configuration manquante: {', '.join(missing)}"
            print(error_msg)
            raise ValueError(error_msg)
        
        print("✅ Configuration Databricks validée!")
        return True
    
    @staticmethod
    def get_connection():
        """Créer une connexion à Databricks"""
        print("🔌 Tentative de connexion à Databricks...")
        
        # Valider d'abord
        DatabricksConfig.validate_config()
        
        try:
            conn = sql.connect(
                server_hostname=DatabricksConfig.SERVER_HOSTNAME,
                http_path=DatabricksConfig.HTTP_PATH,
                access_token=DatabricksConfig.ACCESS_TOKEN
            )
            print("✅ Connexion Databricks établie!")
            return conn
        except Exception as e:
            print(f"❌ Erreur de connexion: {e}")
            raise

def execute_query_safe(query, params=None):
    """
    Version SAFE de execute_query avec gestion robuste des erreurs
    """
    conn = None
    cursor = None
    
    try:
        conn = DatabricksConfig.get_connection()
        cursor = conn.cursor()
        
        print(f"📊 Exécution SAFE de la requête: {query[:80]}...")
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Récupérer les colonnes
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Conversion MANUELLE et SÉCURISÉE
        data = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(columns):
                try:
                    value = row[i]
                    # Gestion robuste des types
                    if value is None:
                        row_dict[col_name] = None
                    elif isinstance(value, (int, float)):
                        row_dict[col_name] = value
                    elif isinstance(value, str):
                        row_dict[col_name] = value
                    else:
                        # Pour les autres types (datetime, etc.), convertir en string
                        row_dict[col_name] = str(value)
                except Exception as col_error:
                    print(f"⚠️ Erreur sur colonne {col_name}: {col_error}")
                    row_dict[col_name] = None
            data.append(row_dict)
        
        print(f"✅ Requête SAFE exécutée: {len(data)} résultats")
        return data
        
    except Exception as e:
        print(f"❌ Erreur SQL SAFE: {e}")
        print(f"Query: {query}")
        return []
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

# Alias pour compatibilité
execute_query = execute_query_safe

def test_connection():
    """Tester la connexion à Databricks avec une requête simple"""
    try:
        print("\n🧪 Test de connexion SAFE à Databricks...")
        conn = DatabricksConfig.get_connection()
        cursor = conn.cursor()
        
        # Requête TEST simple qui ne cause pas de problèmes
        cursor.execute("SELECT 'test_success' as status, 1 as value")
        result = cursor.fetchone()
        
        print(f"✅ Test SAFE réussi: {result}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Test SAFE échoué: {e}")
        return False
    
def execute_query_debug(query, params=None):
    """
    Version DEBUG pour voir exactement la requête exécutée
    """
    print(f"🐛 DEBUG Query: {query}")
    
    if params:
        print(f"🐛 DEBUG Params: {params}")
    
    return execute_query_safe(query, params)

# Test au chargement du module
if __name__ == "__main__":
    print("\n" + "="*50)
    print("🧪 TEST DE CONFIGURATION COMPLET")
    if test_connection():
        print("🎉 Configuration OK!")
    else:
        print("💥 Configuration ÉCHOUÉE!")