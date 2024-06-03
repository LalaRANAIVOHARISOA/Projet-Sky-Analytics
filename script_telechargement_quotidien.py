import requests
import pandas as pd
import datetime
import os
import schedule
import time

# Configuration
base_url = 'http://sc-e.fr/docs/'
repertoire_sauvegarde = r'C:\Users\guill\FORMATION DATA ANALYST\TP\PROJETS\Projet 3\SKY DRIVE\CSV_maj_quotidienne'

# Fonction pour télécharger un fichier
def telecharger_fichier(url_fichier, chemin_sauvegarde):
    response = requests.get(url_fichier)
    if response.status_code == 200:
        with open(chemin_sauvegarde, 'wb') as file:
            file.write(response.content)
        print(f'Téléchargé avec succès: {chemin_sauvegarde}')
    else:
        print(f'Échec du téléchargement: {url_fichier}, Code de statut: {response.status_code}')

# Fonction pour charger un fichier CSV en DataFrame
def charger_csv(chemin_fichier):
    if os.path.exists(chemin_fichier):
        return pd.read_csv(chemin_fichier)
    else:
        print(f'Fichier non trouvé: {chemin_fichier}')
        return pd.DataFrame()

# Fonction principale
def principal():
    # Vérifier si le répertoire de sauvegarde existe, sinon, le créer
    if not os.path.exists(repertoire_sauvegarde):
        os.makedirs(repertoire_sauvegarde)
    
    # Obtenir la date du jour
    aujourdhui = datetime.date.today()
    date_str = aujourdhui.strftime('%Y-%m-%d')
    hier = aujourdhui - datetime.timedelta(days=1)
    hier_str = hier.strftime('%Y-%m-%d')

    # Construire les URLs des fichiers à télécharger
    url_logs = f'{base_url}logs_vols_{date_str}.csv'
    url_degradations = f'{base_url}degradations_{date_str}.csv'

    # Construire les chemins de sauvegarde
    chemin_logs = os.path.join(repertoire_sauvegarde, f'logs_vols_{date_str}.csv')
    chemin_degradations = os.path.join(repertoire_sauvegarde, f'degradations_{date_str}.csv')
    
    # Télécharger les fichiers
    telecharger_fichier(url_logs, chemin_logs)
    telecharger_fichier(url_degradations, chemin_degradations)

    # Charger les nouveaux fichiers avec pandas seulement s'ils existent
    nouveaux_logs = charger_csv(chemin_logs)
    nouvelles_degradations = charger_csv(chemin_degradations)

    if nouveaux_logs.empty or nouvelles_degradations.empty:
        print('Nouveaux fichiers non disponibles ou vides.')
        return

    # Construire les chemins des anciens fichiers
    ancien_chemin_logs = os.path.join(repertoire_sauvegarde, f'logs_vols_{hier_str}.csv')
    ancien_chemin_degradations = os.path.join(repertoire_sauvegarde, f'degradations_{hier_str}.csv')

    # Charger les anciens fichiers
    anciens_logs = charger_csv(ancien_chemin_logs)
    anciennes_degradations = charger_csv(ancien_chemin_degradations)

    # Combiner les nouvelles données avec les anciennes
    logs_combines = pd.concat([anciens_logs, nouveaux_logs]).drop_duplicates().reset_index(drop=True)
    degradations_combines = pd.concat([anciennes_degradations, nouvelles_degradations]).drop_duplicates().reset_index(drop=True)

    # Sauvegarder les fichiers combinés
    chemin_logs_combines = os.path.join(repertoire_sauvegarde, f'logs_vols_{date_str}.csv')
    chemin_degradations_combines = os.path.join(repertoire_sauvegarde, f'degradations_{date_str}.csv')

    logs_combines.to_csv(chemin_logs_combines, index=False)
    degradations_combines.to_csv(chemin_degradations_combines, index=False)

    print(f'Fichiers mis à jour et sauvegardés dans: {repertoire_sauvegarde}')

# Planifie l'exécution de la fonction toutes les 10 minutes
schedule.every(10).minutes.do(principal)

while True:
    schedule.run_pending()
    time.sleep(1)