# Devoir Maison : Spark

MAJ : 11 février, 15h

## Présentation du projet

- Le projet permet de nettoyer et préparer des datasets pour une analyse de données ultérieure.
- La mise en oeuvre du projet se trouve décrite ci-dessous
- Le jobspark 'process.py' se trouve dans le dossier '/data-ingestion-job/src/', il est commenté afin d'en comprendre le fonctionnement
- La dernière partie du jobspark est en cours de débuggage, elle permet de créer de nouvelles tables pour offrir des données supplémentaires aux analystes.

## Mise en place des dossiers

- Objectifs:
   * préparer les dossiers
   * télécharger les datasets
   * préparer les datasets

+ Préparer les dossiers

```bash
mkdir ~/Desktop/DMSpark
mkdir ~/Desktop/DMSpark/datasets
``` 
Puis déplacer/copier votre dossier 'spark-2.4.4-bin-hadoop2.7' dans /DMSpark/

Puis se positionner dans le dossier du DM

```bash
cd ~/Desktop/DMSpark
``` 
Ensuite il faut télécharger le code source :

```bash
git clone git@github.com:AlanJBX/DMSpark.git
mv DMSpark lancement
``` 

+ Télécharger les datasets

Demandes de valeurs foncières géolocalisées: https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres-geolocalisees/

[Lien de téléchargement](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2019/full.csv.gz)

Le fichier téléchargé doit être a ce path : `~/Downloads/full.csv.gz`.

Adresse et géolocalisation des établissements d'enseignement du premier et second degrés: https://www.data.gouv.fr/fr/datasets/adresse-et-geolocalisation-des-etablissements-denseignement-du-premier-et-second-degres-1/

[Lien de téléchargement](https://www.data.gouv.fr/fr/datasets/r/b3b26ad1-a143-4651-afd6-dde3908196fc)

Le fichier téléchargé doit être a ce path : `~/Downloads/fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre.csv`

+ Préparer les datasets

Toujours en étant dans le dossier 'lancement'

```bash
make prepare-dataset
``` 

Vérifier la présence des datasets :

```bash
ls ~/Desktop/DMSpark/datasets
``` 

Vous devriez alors avoir l'architecture suivante :

```bash
~/Desktop/DMSPark
  |- datasets
     |- ecoles.csv
     |- logements.csv
  |- lancement
     |- 'dossier github'
  |- spark-2.4.4-bin-hadoop2.7
     |- 'dossier spark'
``` 

## Lancement du jobspark

- Objectifs:
   * ouvrir le docker
   * lancer le traitement des données
   * récupérer les données et les utiliser

+ Ouvrir le docker

Après avoir vérifier que l'application 'Docker' est allumée et bien configurée 

```bash
docker run --rm -ti -v ~/Desktop/DMSpark/datasets:/data -v $(pwd)/data-ingestion-job/src:/src  -p 4040:4040 --entrypoint bash stebourbi/sio:pyspark'
``` 
Je suis désormais dans le docker. Je vérifie la présence de mes fichiers :

```bash
root@****:/ ls data
```
Je trouve : ecoles.csv et logements.csv

Il faut veiller à ce qu'il n'y ait pas de dossier 'data_ecoles', 'data_logs' ou 'data_out'. Dans le cas contraire :

```bash
root@****:/ rm -rf data/data_*
``` 


```bash
root@****:/ ls /src
``` 

Je trouve : process.py

+ Lancer le traitement des données

Je peux désormais lancer le jobspark.

```bash
root@****:/ /spark-2.4.4-bin-hadoop2.7/bin/spark-submit /src/process.py -l /data/logements.csv -e /data/ecoles.csv -o /data/data_out
```

+ Récupérer les données et les utiliser.

Je quitte mon docker
```bash
exit
```

Je peux récupérer mes données directement sur mon ordinateur à l'adresse :
```bash
ls ~/Desktop/DMSpark/datasets
```

Afin de les utiliser pour traitement, je lance 'pyspark' depuis le dossier 'lancement'
```bash
cd ~/Desktop/DMSpark/lancement
make run-pyspark
```

Je peux désormais traiter les tables via Spark
```python
Table_x = spark.read.parquet("./data/data_x")
```
