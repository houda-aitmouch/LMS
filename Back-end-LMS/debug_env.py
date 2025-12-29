#!/usr/bin/env python3
"""
Debug du fichier .env
"""
import os
from dotenv import load_dotenv

print("🔍 DEBUG DU FICHIER .env")
print("=" * 50)

# 1. Vérifier le chemin actuel
current_dir = os.getcwd()
print(f"📁 Dossier courant: {current_dir}")

# 2. Vérifier si .env existe
env_path = os.path.join(current_dir, '.env')
print(f"📄 Chemin .env: {env_path}")
print(f"📄 .env existe: {os.path.exists(env_path)}")

# 3. Charger et afficher le contenu
if os.path.exists(env_path):
    print("📖 Contenu du fichier .env:")
    with open(env_path, 'r') as f:
        content = f.read()
        print(content)
else:
    print("❌ Fichier .env NON TROUVÉ!")

# 4. Charger avec dotenv
print("\n🔄 Chargement avec dotenv...")
load_dotenv()

# 5. Afficher les variables
print("\n📋 Variables chargées:")
variables = [
    'DATABRICKS_SERVER_HOSTNAME',
    'DATABRICKS_HTTP_PATH', 
    'DATABRICKS_ACCESS_TOKEN'
]

for var in variables:
    value = os.getenv(var)
    if value:
        print(f"✅ {var}: {value[:20]}...")  # Afficher les 20 premiers caractères
    else:
        print(f"❌ {var}: NON DÉFINI")