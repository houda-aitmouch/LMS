# LMS Project – Flask Backend & React Frontend

Ce projet est composé de :
- 🧠 **Backend** : Flask (Python)
- 🎨 **Frontend** : React
- 📊 **Connexion Databricks** (via variables d’environnement)

---

## 🔐 Configuration Databricks

Avant de lancer le projet, vous devez **remplir vos propres informations Databricks**.

Créez un fichier `.env` (ou configurez vos variables d’environnement) et renseignez :

```env
DATABRICKS_SERVER_HOSTNAME=dbc-*********
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/*******
DATABRICKS_ACCESS_TOKEN=YOUR_TOKEN


▶️ Execution
Backend :

bash
python app.py
Frontend :

bash
npx react-scripts start
