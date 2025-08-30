#!/bin/bash

# === CONFIGURATION ===
SPARK_NAMESPACE="spark"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
JOB_NAME="delta-table-reader-${TIMESTAMP}"
SPARK_IMAGE="aladdin7/spark-delta-dbt:3.5.0"
PVC_NAME="pvc-spark"
SCRIPT_NAME="delta_reader.py"
TABLE_PATH="s3a://transactions/raw/delta-data"

echo "=== 📦 DÉMARRAGE DU JOB DELTA TABLE READER ==="
echo "Job Name: ${JOB_NAME}"
echo "Timestamp: ${TIMESTAMP}"

# Vérification de l'existence du script
echo "🔍 Vérification du fichier ${SCRIPT_NAME}..."
kubectl run check-delta-script --rm -i --restart=Never --image=busybox:1.35 \
  --namespace=${SPARK_NAMESPACE} \
  --overrides="{
    \"spec\": {
      \"containers\": [{
        \"name\": \"check-delta-script\",
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
    echo "❌ Erreur: Le fichier ${SCRIPT_NAME} est introuvable dans /data/jobs/"
    exit 1
fi

echo "✅ Script trouvé. Soumission du job Spark..."

# Déploiement du job Spark
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: spark-submit-${JOB_NAME}
  namespace: ${SPARK_NAMESPACE}
  labels:
    app: delta-table-reader
    job-id: ${JOB_NAME}
spec:
  serviceAccountName: spark
  containers:
  - name: spark-submit
    image: ${SPARK_IMAGE}
    command: ["/opt/bitnami/spark/bin/spark-submit"]
    args:
      - "--conf=spark.kubernetes.file.upload.path=file:///data"
      - "--master=k8s://https://kubernetes.default.svc"
      - "--deploy-mode=cluster"
      - "--name=${JOB_NAME}"
      - "--conf=spark.kubernetes.namespace=${SPARK_NAMESPACE}"
      - "--conf=spark.kubernetes.container.image=${SPARK_IMAGE}"
      - "--conf=spark.kubernetes.authenticate.driver.serviceAccountName=spark"
      - "--conf=spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-pvc.options.claimName=${PVC_NAME}"
      - "--conf=spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-pvc.mount.path=/data"
      - "--conf=spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-pvc.options.claimName=${PVC_NAME}"
      - "--conf=spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-pvc.mount.path=/data"
      - "--conf=spark.hadoop.fs.s3a.endpoint=http://minio-api.minio.svc.cluster.local:9000"
      - "--conf=spark.hadoop.fs.s3a.access.key=minioadmin"
      - "--conf=spark.hadoop.fs.s3a.secret.key=minioadmin"
      - "--conf=spark.hadoop.fs.s3a.path.style.access=true"
      - "--conf=spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
      - "--conf=spark.hadoop.fs.s3a.connection.ssl.enabled=false"
      - "--conf=spark.sql.adaptive.enabled=true"
      - "--conf=spark.executor.instances=2"
      - "--conf=spark.executor.memory=1000m"
      - "--conf=spark.executor.cores=1"
      - "--conf=spark.driver.memory=650m"
      - "--conf=spark.sql.adaptive.coalescePartitions.enabled=true"
      - "--conf=spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
      - "--conf=spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
      - "--jars=/data/jars/*.jar"
      - "/data/jobs/${SCRIPT_NAME}"
      - "${TABLE_PATH}"
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
    echo "✅ Job soumis avec succès."
else
    echo "❌ Erreur lors de la création du job Spark"
    exit 1
fi

# Attente du pod
echo "⏳ Attente du démarrage du pod Spark..."
kubectl wait --for=condition=Ready pod/spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE} --timeout=300s

# Affichage des logs
echo "📄 Logs du job Spark (streaming)..."
kubectl logs -f spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}

