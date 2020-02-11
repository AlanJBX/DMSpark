# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse

# Importation des libraries utiles
from functools import reduce
import math
import sys
import pyspark.sql.types as T
from statistics import *

def main():

    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-l', "--fichier_logements", help='fichier entree logements', required=True)
    parser.add_argument('-e', "--fichier_ecoles", help='fichier entree ecoles', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True) # Pour rajouter un paramètre de sortie

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.fichier_logements,args.fichier_ecoles,args.output)


def process(spark, fichier_logements, fichier_ecoles, output):

    ##############################
    # PRISE EN MAIN DES DATASETS #
    ##############################
    print("--------------------------")
    print("PRISE EN MAIN DES DATASETS")
    print("--------------------------")

    # Chargement du dataset lié aux établissements scolaire et nettoyage du nom des colonnes.
    ECL = spark.read.option('header','true').option("delimiter", ";").option('inferSchema','true').csv(fichier_ecoles)
    ECL = ECL.withColumnRenamed('Code établissement','code_etablissement')\
    .withColumnRenamed('Appellation officielle','appellation_officielle')\
    .withColumnRenamed('Dénomination principale','denomination_principale')\
    .withColumnRenamed('Patronyme uai','patronyme_uai')\
    .withColumnRenamed('Secteur Public/Privé','secteur_public_prive')\
    .withColumnRenamed('Adresse','adresse')\
    .withColumnRenamed('Lieu dit','lieudit')\
    .withColumnRenamed('Boite postale','boite_postale')\
    .withColumnRenamed('Code postal','code_postal_ecole')\
    .withColumnRenamed("Localite d'acheminement",'localite_acheminement')\
    .withColumnRenamed('Commune','commune')\
    .withColumnRenamed('Coordonnee X','coordonne_x_ecole')\
    .withColumnRenamed('Coordonnee Y','coordonne_y_ecole')\
    .withColumnRenamed('EPSG','epsg')\
    .withColumnRenamed('Latitude','latitude_ecole')\
    .withColumnRenamed('Longitude','longitude_ecole')\
    .withColumnRenamed("Qualité d'appariement",'qualite_appariement')\
    .withColumnRenamed('Localisation','localisaion')\
    .withColumnRenamed('Code nature','code_nature_ecole')\
    .withColumnRenamed('Nature','nature_ecole')\
    .withColumnRenamed('Code état établissement','code_etat_etablissement')\
    .withColumnRenamed('Etat établissement','etat_etablissement')\
    .withColumnRenamed('Code département','code_departement')\
    .withColumnRenamed('Code région','code_region')\
    .withColumnRenamed('Code académie','code_academie')\
    .withColumnRenamed('Code commune','code_commune')\
    .withColumnRenamed('Département','departement')\
    .withColumnRenamed('Région','region')\
    .withColumnRenamed('Académie','academie')\
    .withColumnRenamed('Position','position')\
    .withColumnRenamed('secteur_prive_code_type_contrat','secteur_prive_code_type_contrat')\
    .withColumnRenamed('secteur_prive_libelle_type_contrat','secteur_prive_libelle_type_contrat')

    #>>> ECL.printSchema()
    #root
    # |-- code_etablissement: string (nullable = true)
    # |-- appellation_officielle: string (nullable = true)
    # |-- denomination_principale: string (nullable = true)
    # |-- patronyme_uai: string (nullable = true)
    # |-- secteur_public_prive: string (nullable = true)
    # |-- adresse: string (nullable = true)
    # |-- lieudit: string (nullable = true)
    # |-- boite_postale: string (nullable = true)
    # |-- code_postal_ecole: integer (nullable = true)
    # |-- localite_acheminement: string (nullable = true)
    # |-- commune: string (nullable = true)
    # |-- coordonne_x_ecole: double (nullable = true)
    # |-- coordonne_y_ecole: double (nullable = true)
    # |-- epsg: string (nullable = true)
    # |-- latitude_ecole: double (nullable = true)
    # |-- longitude_ecole: double (nullable = true)
    # |-- qualite_appariement: string (nullable = true)
    # |-- localisaion: string (nullable = true)
    # |-- code_nature_ecole: integer (nullable = true)
    # |-- nature_ecole: string (nullable = true)
    # |-- code_etat_etablissement: integer (nullable = true)
    # |-- etat_etablissement: string (nullable = true)
    # |-- code_departement: string (nullable = true)
    # |-- code_region: integer (nullable = true)
    # |-- code_academie: integer (nullable = true)
    # |-- code_commune: string (nullable = true)
    # |-- departement: string (nullable = true)
    # |-- region: string (nullable = true)
    # |-- academie: string (nullable = true)
    # |-- position: string (nullable = true)
    # |-- secteur_prive_code_type_contrat: integer (nullable = true)
    # |-- secteur_prive_libelle_type_contrat: string (nullable = true)

    # Chargement du dataset lié aux parcelles et nettoyage du noms des colonnes.
    LOG = spark.read.option('header','true').option('inferSchema','true').csv(fichier_logements)
    LOG = LOG.withColumnRenamed('longitude','longitude_log') \
    .withColumnRenamed('latitude','latitude_log')

    #>>> LOG.printSchema()
    #root
    # |-- id_mutation: string (nullable = true)
    # |-- date_mutation: timestamp (nullable = true)
    # |-- numero_disposition: integer (nullable = true)
    # |-- nature_mutation: string (nullable = true)
    # |-- valeur_fonciere: double (nullable = true)
    # |-- adresse_numero: integer (nullable = true)
    # |-- adresse_suffixe: string (nullable = true)
    # |-- adresse_nom_voie: string (nullable = true)
    # |-- adresse_code_voie: string (nullable = true)
    # |-- code_postal: integer (nullable = true)
    # |-- code_commune: string (nullable = true)
    # |-- nom_commune: string (nullable = true)
    # |-- code_departement: string (nullable = true)
    # |-- ancien_code_commune: integer (nullable = true)
    # |-- ancien_nom_commune: string (nullable = true)
    # |-- id_parcelle: string (nullable = true)
    # |-- ancien_id_parcelle: string (nullable = true)
    # |-- numero_volume: string (nullable = true)
    # |-- lot1_numero: string (nullable = true)
    # |-- lot1_surface_carrez: double (nullable = true)
    # |-- lot2_numero: string (nullable = true)
    # |-- lot2_surface_carrez: double (nullable = true)
    # |-- lot3_numero: string (nullable = true)
    # |-- lot3_surface_carrez: double (nullable = true)
    # |-- lot4_numero: integer (nullable = true)
    # |-- lot4_surface_carrez: double (nullable = true)
    # |-- lot5_numero: integer (nullable = true)
    # |-- lot5_surface_carrez: double (nullable = true)
    # |-- nombre_lots: integer (nullable = true)
    # |-- code_type_local: integer (nullable = true)
    # |-- type_local: string (nullable = true)
    # |-- surface_reelle_bati: integer (nullable = true)
    # |-- nombre_pieces_principales: integer (nullable = true)
    # |-- code_nature_culture: string (nullable = true)
    # |-- nature_culture: string (nullable = true)
    # |-- code_nature_culture_speciale: string (nullable = true)
    # |-- nature_culture_speciale: string (nullable = true)
    # |-- surface_terrain: integer (nullable = true)
    # |-- longitude_log: double (nullable = true)
    # |-- latitude_log: double (nullable = true)

    ######################
    # DEFINITION DES UDF #
    ######################
    print("------------------")
    print("DEFINITION DES UDF")
    print("------------------")

    # Fonction permettant de déterminer le niveau scolaire d'un établissement en fonction de son code_nature_ecole
    def degre(col):
        MATERNELLE = [101,102,103,111]
        PRIMAIRE = [151,152,153,154]
        COLLEGE = [340,342,350,352]
        SPECIAL = [160,162,169,170,312,315,349,370,390]
        LYCEE = [300,301,302,306,310,320,332,334,335,344]
        if int(col) in MATERNELLE:
            return 'MATERNELLE'
        elif int(col) in PRIMAIRE:
            return 'PRIMAIRE'
        elif int(col) in COLLEGE:
            return 'COLLEGE'
        elif int(col) in LYCEE:
            return 'LYCEE'
        elif int(col) in SPECIAL:
            return 'SPECIAL' 
        else:
            return 'null'

    # Fonction permettant de déterminer la distance entre deux points en fonction de leurs longitudes et latitudes respectives.
    def distance(latitude1,longitude1,latitude2,longitude2):
        # Calcul de la distance selon la formule de Haversine
        R = 6371.0
        lat1 = math.radians(latitude1)
        lon1 = math.radians(longitude1)
        lat2 = math.radians(latitude2)
        lon2 = math.radians(longitude2)
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return round(distance)

    # Fonction permettant de déterminer la valeur maximum de plusieurs colonnes sur une même ligne
    def row_max(*cols):
        return reduce(lambda x, y: F.when(x > y, x).otherwise(y),[F.col(c) if isinstance(c, str) else c for c in cols])

    # Afin de définir les fonctions comme "user defined function"
    UDFdegre = F.udf(degre)
    UDFdistance = F.udf(distance)

    ################################
    # NETTOYAGE DU DATASET "ECOLE" #
    ################################
    print("--------------------------")
    print("NETTOYAGE DU DATASET ECOLE")
    print("--------------------------")

    # Je crée la colonne qui détermine le degré de scolarité.
    ecoles = ECL.withColumn('degre_etude',UDFdegre(ECL['code_nature_ecole']))

    # Je garde les informations lisibles (pas de code humainement illisible et information non parlante)
    # Je garde uniquement le code_etablissment me permettant éventuellement de retrouver les données dans la table initiale
    ecoles = ecoles.select("code_etablissement","appellation_officielle","code_postal_ecole","secteur_public_prive","degre_etude","longitude_ecole","latitude_ecole")

    # Je supprime les valeurs null qui impacteront les analyses sur la distance entre les logements et les écoles
    # Le choix est arbitraire. Il est également envisageable d'étudier le lien entre les entités par les communes plutôt que par les distances
    ecoles = ecoles.dropna(subset=("longitude_ecole","latitude_ecole"))

    # Je renomme ma variable pour plus de lisibilité et de facilité à rentrer les commandes.
    data_ecoles = ecoles

    # Je sauvegarde le dataset relatif aux écoles nettoyées et prêtes à l'analyse
    ########################################
    data_ecoles.write.parquet('/data/data_ecoles')
    ########################################
    # Pour ouvrir le fichier plus tard dans Spark : spark.read.parquet("./data/data_ecoles")
    print("..........")
    print("Sauvegarde")
    print("..........")

    #>>> data_ecoles.printSchema()
    #root
    # |-- code_etablissement: string (nullable = true)
    # |-- appellation_officielle: string (nullable = true)
    # |-- code_postal_ecole : integer (nullable = true)
    # |-- secteur_public_prive: string (nullable = true)
    # |-- longitude_ecole: double (nullable = true)
    # |-- latitude_ecole: double (nullable = true)
    # |-- degre_etude: string (nullable = true)

    ####################################    
    # NETTOYAGE DU DATASET "LOGEMENTS" #
    ####################################
    print("------------------------------")
    print("NETTOYAGE DU DATASET LOGEMENTS")
    print("------------------------------")

    # Je garde les informations lisibles (pas de code humainement illisible et information non parlante)
    # I.E : uniquement les logements ('Maison' et 'Appartement'), supérieur à 9m2 (selon la législation française)
    logements = LOG.select("id_parcelle","longitude_log","latitude_log","type_local","adresse_numero","adresse_suffixe","adresse_nom_voie","code_postal","nom_commune","surface_reelle_bati","surface_terrain","valeur_fonciere").where((LOG.type_local == 'Appartement') | (LOG.type_local == 'Maison')).where(LOG.surface_reelle_bati >= 9)

    # Je supprime les valeurs null et nettoie celles qui le nécessitent
    logementsV2 = logements.dropna(subset=('surface_reelle_bati','valeur_fonciere','longitude_log','latitude_log')).fillna({'surface_terrain':0}).fillna({'surface_terrain':0})
    logementsV2 = logementsV2.withColumn('surface_fonciere',row_max('surface_reelle_bati','surface_terrain')) # Surface réelle du terrain
    logementsV2 = logementsV2.drop('surface_reelle_bati','surface_terrain')

    # Je défini le prix du mètre carré du terrain.
    logementsV3 = logementsV2.withColumn('prix_metre_carre',logementsV2['valeur_fonciere']/logementsV2['surface_fonciere'])
    # Certains prix sont 'sales' : prix au mètre carré = 10^-5 euros ?!

    # Je renomme ma variable pour plus de lisibilité et de facilité à rentrer les commandes.
    data_log = logementsV3

    # Je sauvegarde le dataset relatif aux logements nettoyés et prêts à l'analyse
    ########################################
    data_log.write.parquet('/data/data_log')
    ########################################
    # Pour ouvrir le fichier plus tard dans Spark  : spark.read.parquet("./data/data_log")
    print("..........")
    print("Sauvegarde")
    print("..........")

    #>>> data_log.printSchema()
    #root
    # |-- id_parcelle: string (nullable = true)
    # |-- longitude_log: double (nullable = true)
    # |-- latitude_log: double (nullable = true)
    # |-- type_local: string (nullable = true)
    # |-- adresse_numero: integer (nullable = true)
    # |-- adresse_suffixe: string (nullable = true)
    # |-- adresse_nom_voie: string (nullable = true)
    # |-- code_postal : integer (nullable = true)
    # |-- nom_commune: string (nullable = true)
    # |-- valeur_fonciere: double (nullable = true)
    # |-- surface_fonciere: integer (nullable = true)
    # |-- prix_metre_carre: double (nullable = true)

    ###############################
    # CREATION DE TABLES ANNEXES #
    ###############################
    print("--------------------------")
    print("CREATION DE TABLES ANNEXES")
    print("--------------------------")

    # Choix arbitraire de département afin de réduire la taille du dataset et permettre les tests.
    data_ecoles = data_ecoles.where((data_ecoles.code_postal_ecole >= 56000) & (data_ecoles.code_postal_ecole <= 56999))
    data_log = data_log.where((data_log.code_postal >= 56000) & (data_log.code_postal <= 56999))
    # En modifiant les tables, il est possible de faire la sélection sur les régions, les académies,...

    # Requete cross-join liant les logements aux écoles distants maxium de 50km
    # La valeur "50" est fixée de manière arbitraire.
    log_cross_ecoles = data_log.crossJoin(data_ecoles).withColumn('distance',UDFdistance(data_log.latitude_log,data_log.longitude_log,data_ecoles.latitude_ecole,data_ecoles.longitude_ecole).cast(T.IntegerType()))
    log_cross_ecolesV2 = log_cross_ecoles.where(log_cross_ecoles.distance <= 50)

    # Je nettoye mes données en supprimant les latitudes et longitudes qui ne servent plus à rien après le calcul de la distante.
    log_cross_ecolesV2 = log_cross_ecolesV2.drop('latitude_ecole','longitude_ecole','latitude_log','longitude_log')

    #>>> log_cross_ecolesV2.printSchema()
    #root
    # |-- id_parcelle: string (nullable = true)
    # |-- type_local: string (nullable = true)
    # |-- adresse_numero: integer (nullable = true)
    # |-- adresse_suffixe: string (nullable = true)
    # |-- adresse_nom_voie: string (nullable = true)
    # |-- code_postal: integer (nullable = true)
    # |-- nom_commune: string (nullable = true)
    # |-- valeur_fonciere: double (nullable = true)
    # |-- surface_fonciere: integer (nullable = true)
    # |-- prix_metre_carre: double (nullable = true)
    # |-- code_etablissement: string (nullable = true)
    # |-- appellation_officielle: string (nullable = true)
    # |-- code_postal_ecole : integer (nullable = true)
    # |-- secteur_public_prive: string (nullable = true)
    # |-- degre_etude: string (nullable = true)
    # |-- distance: double (nullable = true)

    # Je renomme ma variable pour plus de lisibilité  et de facilité à rentrer les commandes.
    data_cross = log_cross_ecolesV2

    # Je sauvegarde le dataset
    ########################################
    data_cross.write.parquet('/data/data_cross')
    ########################################
    # Pour ouvrir le fichier plus tard : spark.read.parquet("./data/data_cross")
    print("..........")
    print("Sauvegarde")
    print("..........")

    # Je calcule le prix moyen des logements à 50km à la ronde des établissements
    moyenne_tarifs = data_cross.groupby(data_cross.code_etablissement).agg(F.mean(data_cross.prix_metre_carre).alias('moyenne_tarifs'))
    moyenne_tarifs = moyenne_tarifs.join(data_ecoles,"code_etablissement")

    # Je sauvegarde le dataset
    ########################################
    moyenne_tarifs.write.parquet('/data/data_tarifs')
    ########################################
    # Pour ouvrir le fichier plus tard : spark.read.parquet("./data/data_tarifs")
    print("..........")
    print("Sauvegarde")
    print("..........")

    ##### Ce bloc de commande ne fonctionne actuellement pas #####
    # On recherche les écoles les plus proches de chaque logement et on les unie dans une même table.
    maternelle_proche = data_cross.where((data_cross.degre_etude == 'MATERNELLE') & (data_cross.distance == min(data_cross.distance)))
    primaire_proche = data_cross.where((data_cross.Degre_etude == 'PRIMAIRE') & (data_cross.Distance == min(data_cross.Distance)))
    college_proche = data_cross.where((data_cross.Degre_etude == 'COLLEGE') & (data_cross.Distance == min(data_cross.Distance)))
    lycee_proche = data_cross.where((data_cross.Degre_etude == 'LYCEE') & (data_cross.Distance == min(data_cross.Distance)))
    ecoles_proches = ((maternelle_proche.union(primaire_proche)).union(college_proche)).union(lycee_proche)
    ecoles_proches.printSchema()
    ##### Ce bloc de commande ne fonctionne actuellement pas #####

if __name__ == '__main__':
    main()
