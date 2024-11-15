import shutil
import os
import time
import sys

# Dossier source
source = 'TD4/data/adidas/adidas_stream/'
# Dossier de destination
destination = 'TD4/data/adidas/stream/'

# Définition du temps de traitement à partir d'un argument de ligne de commande ou par défaut à 1 seconde
processingTime = int(sys.argv[1]) if len(sys.argv) > 1 else 1

dossier = os.listdir(source)

for fichier in dossier:
    shutil.copy(os.path.join(source, fichier), destination) # copie des fichiers de la source vers la destination
    time.sleep(processingTime) # attend le temps défini entre chaque transfert
