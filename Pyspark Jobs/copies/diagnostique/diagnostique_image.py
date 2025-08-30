from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
import json

def create_spark_session():
    """Créer une session Spark avec les configurations nécessaires pour Delta Lake"""
    spark = SparkSession.builder \
        .appName("CustomerImagesDiagnosticTool") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-api.minio.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def diagnose_json_files(spark):
    """Diagnostiquer les fichiers JSON pour comprendre le problème"""
    print("=" * 80)
    print("🔍 DIAGNOSTIC DES FICHIERS JSON CUSTOMER IMAGES")
    print("=" * 80)
    
    json_source_path = "s3a://customer-images/raw"
    
    try:
        print(f"📁 Analyse du dossier: {json_source_path}")
        
        # 1. Découvrir tous les fichiers JSON
        print("\n1️⃣ DÉCOUVERTE DES FICHIERS...")
        
        json_files = []
        try:
            # Méthode récursive pour trouver tous les fichiers
            temp_df = spark.read.option("recursiveFileLookup", "true").text(json_source_path)
            input_files = temp_df.inputFiles()
            json_files = [f for f in input_files if (f.endswith('.json') or f.endswith('.json.gz')) and not f.endswith('_SUCCESS')]
            print(f"   ✅ {len(json_files)} fichiers JSON trouvés")
        except Exception as e:
            print(f"   ❌ Erreur: {str(e)}")
            return
        
        if not json_files:
            print("   ⚠️ Aucun fichier JSON trouvé")
            return
        
        # 2. Analyser chaque fichier individuellement
        print(f"\n2️⃣ ANALYSE DÉTAILLÉE DES FICHIERS...")
        
        total_records = 0
        
        for i, file_path in enumerate(json_files[:10]):  # Analyser maximum 10 fichiers pour le diagnostic
            print(f"\n📄 Fichier {i+1}: {file_path.split('/')[-1]}")
            
            try:
                # Méthode 1: Lecture avec schéma automatique (plus flexible)
                print(f"   🔄 Méthode 1: Lecture automatique...")
                
                if file_path.endswith('.json.gz'):
                    df_auto = spark.read \
                        .option("multiline", "true") \
                        .option("mode", "PERMISSIVE") \
                        .option("compression", "gzip") \
                        .json(file_path)
                else:
                    df_auto = spark.read \
                        .option("multiline", "true") \
                        .option("mode", "PERMISSIVE") \
                        .json(file_path)
                
                count_auto = df_auto.count()
                print(f"   📊 Nombre de lignes (auto): {count_auto}")
                
                if count_auto > 0:
                    print(f"   📋 Colonnes détectées: {df_auto.columns}")
                    print(f"   🔍 Schéma:")
                    df_auto.printSchema()
                    
                    # Afficher quelques exemples
                    print(f"   📝 Exemple de données:")
                    df_auto.show(2, truncate=True)
                    
                    total_records += count_auto
                else:
                    print(f"   ⚠️ Fichier vide ou non lisible")
                
                # Méthode 2: Lecture en tant que texte brut pour voir le contenu réel
                print(f"   🔄 Méthode 2: Lecture en texte brut...")
                
                if file_path.endswith('.json.gz'):
                    text_df = spark.read \
                        .option("compression", "gzip") \
                        .text(file_path)
                else:
                    text_df = spark.read.text(file_path)
                
                text_lines = text_df.count()
                print(f"   📊 Nombre de lignes de texte: {text_lines}")
                
                if text_lines > 0:
                    print(f"   📝 Premières lignes de texte:")
                    sample_lines = text_df.limit(3).collect()
                    for j, row in enumerate(sample_lines):
                        line_preview = row.value[:100] + "..." if len(row.value) > 100 else row.value
                        print(f"      Ligne {j+1}: {line_preview}")
                
            except Exception as file_error:
                print(f"   ❌ Erreur lors de l'analyse: {str(file_error)}")
        
        print(f"\n📊 RÉSUMÉ:")
        print(f"   Total fichiers analysés: {min(len(json_files), 10)}")
        print(f"   Total fichiers disponibles: {len(json_files)}")
        print(f"   Total enregistrements trouvés: {total_records}")
        
        if len(json_files) > 10:
            print(f"   ⚠️ {len(json_files) - 10} fichiers supplémentaires non analysés (limitation)")
        
        # 3. Test de lecture globale avec différentes approches
        print(f"\n3️⃣ TEST DE LECTURE GLOBALE...")
        
        try:
            print("   🔄 Lecture globale avec schéma automatique...")
            global_df = spark.read \
                .option("recursiveFileLookup", "true") \
                .option("multiline", "true") \
                .option("mode", "PERMISSIVE") \
                .json(json_source_path)
            
            global_count = global_df.count()
            print(f"   📊 Total enregistrements (lecture globale): {global_count}")
            
            if global_count > 0:
                print(f"   📋 Colonnes détectées globalement: {global_df.columns}")
                print(f"   📝 Échantillon global:")
                global_df.show(5, truncate=True)
            
        except Exception as global_error:
            print(f"   ❌ Erreur lecture globale: {str(global_error)}")
        
        # 4. Vérifier les fichiers traités
        print(f"\n4️⃣ VÉRIFICATION DES MÉTADONNÉES DE TRAITEMENT...")
        
        try:
            processed_files_path = "s3a://customer-images/metadata/processed_json_files.json"
            processed_df = spark.read.json(processed_files_path)
            processed_count = processed_df.count()
            print(f"   📊 Fichiers marqués comme traités: {processed_count}")
            
            if processed_count > 0:
                print(f"   📋 Liste des fichiers traités:")
                processed_df.select("file_path", "record_count").show(truncate=False)
        except Exception as metadata_error:
            print(f"   ⚠️ Pas de métadonnées de traitement trouvées: {str(metadata_error)}")
        
    except Exception as e:
        print(f"❌ Erreur générale lors du diagnostic: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Fonction principale de diagnostic"""
    print("🚀 DÉMARRAGE DU DIAGNOSTIC JSON CUSTOMER IMAGES")
    
    spark = create_spark_session()
    
    try:
        # Diagnostic complet
        diagnose_json_files(spark)
        
        print(f"\n✅ DIAGNOSTIC CUSTOMER IMAGES TERMINÉ")
        print(f"\n💡 RECOMMANDATIONS:")
        print(f"   1. Vérifiez si le nombre total d'enregistrements correspond à vos attentes")
        print(f"   2. Si les fichiers contiennent plus de données d'images, le problème peut être:")
        print(f"      - Schéma trop rigide dans le script original")
        print(f"      - Filtres trop restrictifs")
        print(f"      - Mauvaise gestion des fichiers déjà traités")
        print(f"   3. Utilisez ce diagnostic pour identifier la vraie cause")
        
    except Exception as e:
        print(f"❌ Erreur lors du diagnostic: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("🔚 Fermeture de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    main()
