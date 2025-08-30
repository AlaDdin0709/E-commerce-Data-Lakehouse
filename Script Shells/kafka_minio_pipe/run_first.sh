#!/bin/bash

# Script pour pré-télécharger les JAR CORRECTS dans le PVC
SPARK_NAMESPACE="spark"
PVC_NAME="pvc-spark"

echo "=== PRÉ-TÉLÉCHARGEMENT DES DÉPENDANCES JAR CORRIGÉES ==="

cat > /tmp/jar-downloader-fixed.yaml <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: jar-downloader-fixed
  namespace: ${SPARK_NAMESPACE}
spec:
  containers:
  - name: jar-downloader
    image: maven:3.8.6-openjdk-11
    command: ["/bin/bash"]
    args:
    - "-c"
    - |
      mkdir -p /data/jars && cd /data/jars
      
      echo '🔄 Nettoyage des anciens JAR...'
      rm -f *.jar
      
      echo '📥 Téléchargement des JAR avec TOUTES les dépendances...'
      
      # JAR principal Kafka-Spark (3.5.0) + toutes ses dépendances
      echo 'Téléchargement spark-sql-kafka et dépendances...'
      wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
      wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar
      
      # Client Kafka et ses dépendances (3.5.1)
      echo 'Téléchargement kafka-clients et dépendances...'
      wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar
      wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/3.5.1/kafka_2.12-3.5.1.jar
      
      # Dépendances Kafka manquantes
      echo 'Téléchargement des dépendances Kafka...'
      wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
      wget -q https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-1/zstd-jni-1.5.5-1.jar
      wget -q https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar
      wget -q https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.1/snappy-java-1.1.10.1.jar
      
      # JAR pour MinIO/S3
      echo 'Téléchargement hadoop-aws...'
      wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
      wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar
      wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar
      
      echo 'Téléchargement aws-java-sdk-bundle...'
      wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
      
      echo '✅ Téléchargement terminé. Fichiers présents:'
      ls -la /data/jars/
      
      echo '📊 Tailles des fichiers:'
      du -h /data/jars/*.jar
      
      echo '🔍 Vérification du contenu des JAR Kafka:'
      jar -tf spark-sql-kafka-0-10_2.12-3.5.0.jar | grep -i ByteArraySerializer || echo 'ByteArraySerializer non trouvé dans spark-sql-kafka'
      jar -tf kafka-clients-3.5.1.jar | grep -i ByteArraySerializer || echo 'ByteArraySerializer non trouvé dans kafka-clients'
      
    volumeMounts:
    - name: data-volume
      mountPath: /data
  volumes:
  - name: data-volume
    persistentVolumeClaim:
      claimName: ${PVC_NAME}
  restartPolicy: Never
EOF

kubectl apply -f /tmp/jar-downloader-fixed.yaml

echo "⏳ Attente du pod..."
kubectl wait --for=condition=Ready pod/jar-downloader-fixed -n ${SPARK_NAMESPACE} --timeout=300s

echo "📋 Logs du téléchargement:"
kubectl logs -f jar-downloader-fixed -n ${SPARK_NAMESPACE}

echo "🧹 Nettoyage du pod temporaire..."
kubectl delete pod jar-downloader-fixed -n ${SPARK_NAMESPACE}
rm /tmp/jar-downloader-fixed.yaml

echo "✅ JAR CORRIGÉS téléchargés dans le PVC!"
echo ""
echo "🔧 PROCHAINES ÉTAPES:"
echo "1. Vérifiez que tous les JAR sont présents"
echo "2. Exécutez le job Spark avec les nouveaux paramètres"
echo "3. Le job devrait maintenant fonctionner sans erreur ClassNotFoundException"
