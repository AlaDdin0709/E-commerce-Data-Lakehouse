#!/bin/bash

# Variables
SPARK_NAMESPACE="spark"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
JOB_NAME="kafka-minio-batch-${TIMESTAMP}"
SPARK_IMAGE="aladdin7/spark-delta-dbt:3.5.0"
PVC_NAME="pvc-spark"
SCRIPT_NAME="iot_sensors_processor.py"

echo "=== DÉMARRAGE DU JOB KAFKA VERS MINIO ==="
echo "Job Name: ${JOB_NAME}"
echo "Timestamp: ${TIMESTAMP}"

# Vérifier que le fichier existe
echo "Vérification de l'existence du fichier ${SCRIPT_NAME}..."
kubectl run temp-check --rm -i --restart=Never --image=busybox:1.35 \
  --namespace=${SPARK_NAMESPACE} \
  --overrides="{
    \"spec\": {
      \"containers\": [{
        \"name\": \"temp-check\",
        \"image\": \"busybox:1.35\",
        \"command\": [\"ls\", \"-la\", \"/data/jobs/${SCRIPT_NAME}\"],
        \"volumeMounts\": [{
          \"name\": \"data-volume\",
          \"mountPath\": \"/data\"
        }]
      }],
      \"volumes\": [{
        \"name\": \"data-volume\",
        \"persistentVolumeClaim\": {
          \"claimName\": \"${PVC_NAME}\"
        }
      }]
    }
  }" -- ls -la /data/jobs/${SCRIPT_NAME}

if [ $? -ne 0 ]; then
    echo "Erreur: Le fichier ${SCRIPT_NAME} n'existe pas dans /data/jobs/!"
    echo "Assurez-vous que le fichier est présent dans le PVC."
    exit 1
fi

echo "✅ Fichier trouvé, création du job Spark..."

# Créer et soumettre le job Spark
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: spark-submit-${JOB_NAME}
  namespace: ${SPARK_NAMESPACE}
  labels:
    app: kafka-minio-batch
    job-id: ${JOB_NAME}
spec:
  serviceAccountName: spark
  containers:
  - name: spark-submit
    image: ${SPARK_IMAGE}
    command: ["/opt/bitnami/spark/bin/spark-submit"]
    args:
      - "--master=k8s://https://kubernetes.default.svc"
      - "--conf=spark.kubernetes.file.upload.path=file:///data"
      - "--name=${JOB_NAME}"
      - "--conf=spark.kubernetes.namespace=${SPARK_NAMESPACE}"
      - "--conf=spark.kubernetes.container.image=${SPARK_IMAGE}"
      - "--conf=spark.kubernetes.authenticate.driver.serviceAccountName=spark"
      - "--conf=spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-pvc.options.claimName=${PVC_NAME}"
      - "--conf=spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-pvc.mount.path=/data"
      - "--conf=spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-pvc.options.claimName=${PVC_NAME}"
      - "--conf=spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-pvc.mount.path=/data"
      - "--conf=spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
      - "--conf=spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
      - "--conf=spark.eventLog.enabled=false"
      - "--conf=spark.executor.instances=2"
      - "--conf=spark.executor.memory=2500m"
      - "--conf=spark.executor.cores=1"
      - "--conf=spark.driver.memory=1000m"
      - "--jars=/data/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/data/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/data/jars/kafka-clients-3.5.1.jar,/data/jars/kafka_2.12-3.5.1.jar,/data/jars/hadoop-aws-3.3.4.jar,/data/jars/aws-java-sdk-bundle-1.12.262.jar,/data/jars/commons-pool2-2.11.1.jar,/data/jars/hadoop-common-3.3.4.jar,/data/jars/zstd-jni-1.5.5-1.jar,/data/jars/lz4-java-1.8.0.jar,/data/jars/snappy-java-1.1.10.1.jar"
      - "--conf=spark.hadoop.fs.s3a.endpoint=http://minio-api.minio.svc.cluster.local:9000"
      - "--conf=spark.hadoop.fs.s3a.access.key=minioadmin"
      - "--conf=spark.hadoop.fs.s3a.secret.key=minioadmin"
      - "--conf=spark.hadoop.fs.s3a.path.style.access=true"
      - "--conf=spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
      - "--conf=spark.hadoop.fs.s3a.connection.ssl.enabled=false"
      - "--conf=spark.sql.adaptive.enabled=true"
      - "--conf=spark.sql.adaptive.coalescePartitions.enabled=true"
      - "--deploy-mode=cluster"
      - "--verbose"
      - "/data/jobs/${SCRIPT_NAME}"
    env:
    - name: KAFKA_BROKERS
      value: "kafka-service:9092"
    - name: MINIO_ENDPOINT  
      value: "http://minio-api.minio.svc.cluster.local:9000"
    - name: MINIO_BUCKET
      value: "transactions"
    - name: SPARK_DRIVER_MEMORY
      value: "1500m"
    - name: SPARK_EXECUTOR_MEMORY
      value: "2000m"
    volumeMounts:
    - name: data-volume
      mountPath: /data
    resources:
      requests:
        memory: "300Mi"
        cpu: "150m"
      limits:
        memory: "500Mi"
        cpu: "300m"
  volumes:
  - name: data-volume
    persistentVolumeClaim:
      claimName: ${PVC_NAME}
  restartPolicy: Never
EOF

if [ $? -eq 0 ]; then
    echo "✅ Job Spark créé avec succès!"
else
    echo "❌ Erreur lors de la création du job Spark"
    exit 1
fi

# Attendre que le pod soit prêt
echo "Attente du démarrage du pod..."
kubectl wait --for=condition=Ready pod/spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE} --timeout=300s

if [ $? -ne 0 ]; then
    echo "⚠️  Timeout lors de l'attente du pod. Vérification du statut..."
    kubectl get pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}
    kubectl describe pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}
fi

# Suivre les logs
echo "📋 Affichage des logs du job..."
kubectl logs -f spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE} &
LOGS_PID=$!

# Fonction pour nettoyer en cas d'interruption
cleanup() {
    echo "🛑 Interruption détectée, nettoyage..."
    kill $LOGS_PID 2>/dev/null
    echo "Statut final du job:"
    kubectl get pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}
    exit 1
}

trap cleanup INT TERM

# Attendre la fin du job
echo "⏳ Attente de la fin du job..."
while true; do
    STATUS=$(kubectl get pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE} -o jsonpath='{.status.phase}' 2>/dev/null)
    
    case $STATUS in
        "Succeeded")
            echo "✅ Job terminé avec succès!"
            break
            ;;
        "Failed")
            echo "❌ Job échoué!"
            kubectl describe pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}
            break
            ;;
        "Pending"|"Running")
            sleep 10
            ;;
        *)
            echo "⚠️  Statut inconnu: $STATUS"
            sleep 10
            ;;
    esac
done

# Arrêter le suivi des logs
kill $LOGS_PID 2>/dev/null

# Afficher le statut final et les informations
echo "📊 RÉSUMÉ FINAL:"
echo "==============="
kubectl get pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}

echo ""
echo "🎯 Pods Spark créés pour ce job:"
kubectl get pods -n ${SPARK_NAMESPACE} -l spark-app-name=${JOB_NAME}

echo ""
echo "📈 Logs finaux (dernières 20 lignes):"
kubectl logs --tail=20 spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}

# Optionnel: nettoyer le pod de soumission après un délai
echo ""
echo "🧹 Le pod de soumission sera conservé pour inspection."
echo "Pour le supprimer manuellement: kubectl delete pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}"

echo "=== FIN DU JOB KAFKA VERS MINIO ==="
