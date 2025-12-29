#!/usr/bin/env python3
"""
Test SAFE des requêtes
"""
from config import execute_query_safe

print("🧪 TEST SAFE DES REQUÊTES")
print("=" * 50)

# Test de différentes requêtes
test_queries = [
    "SELECT year FROM default.dim_date WHERE year IS NOT NULL LIMIT 3",
    "SELECT lms_source FROM default.dim_lms LIMIT 3", 
    "SELECT domain_name FROM default.dim_domain LIMIT 3",
    "SELECT 'test' as simple_column, 123 as number"
]

for i, query in enumerate(test_queries, 1):
    print(f"\n{i}. Query: {query}")
    results = execute_query_safe(query)
    print(f"   Résultats: {len(results)}")
    for row in results:
        print(f"   - {row}")

print("\n✅ Test SAFE terminé!")