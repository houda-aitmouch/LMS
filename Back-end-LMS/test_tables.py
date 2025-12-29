#!/usr/bin/env python3
"""
Test de la structure des tables Delta Lake
"""
from config import execute_query

print("🧪 TEST DES TABLES DELTA LAKE")
print("=" * 50)

tables = [
    'dim_date',
    'dim_lms', 
    'dim_domain',
    'dim_course',
    'fact_learning'
]

for table in tables:
    print(f"\n📊 Table: {table}")
    print("-" * 30)
    
    try:
        # Voir la structure
        describe_query = f"DESCRIBE default.{table}"
        structure = execute_query(describe_query)
        
        print("Structure:")
        for col in structure[:10]:  # Afficher les 10 premières colonnes
            print(f"  {col.get('col_name', 'N/A')} | {col.get('data_type', 'N/A')}")
        
        # Compter les lignes
        count_query = f"SELECT COUNT(*) as count FROM default.{table}"
        count_result = execute_query(count_query)
        print(f"Lignes: {count_result[0]['count'] if count_result else 'N/A'}")
        
        # Aperçu des données (premières lignes)
        if table == 'dim_date':
            sample_query = f"SELECT * FROM default.{table} LIMIT 5"
            sample_data = execute_query(sample_query)
            print("Aperçu:")
            for row in sample_data:
                print(f"  {row}")
                
    except Exception as e:
        print(f"❌ Erreur: {e}")

print("\n" + "=" * 50)
print("✅ Test des tables terminé!")