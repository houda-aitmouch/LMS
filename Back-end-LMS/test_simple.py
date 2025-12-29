#!/usr/bin/env python3
"""
Test de la génération de graphiques
"""
from services.data_service import get_chart_data

print("🧪 TEST GÉNÉRATION GRAPHIQUES")
print("=" * 50)

# Test avec des filtres simples
test_filters = {
    'lms_source': 'Blackboard',
    'year': 2023,
    'domain': 'Business'
}

try:
    results = get_chart_data(
        metric='completion_rate_percent',
        dimension='domain',
        filters=test_filters
    )
    
    print(f"✅ Résultats: {len(results)}")
    for i, row in enumerate(results[:5]):  # Afficher les 5 premiers
        print(f"   {i+1}. {row}")
        
except Exception as e:
    print(f"❌ Erreur: {e}")