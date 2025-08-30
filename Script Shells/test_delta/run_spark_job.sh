#!/bin/bash

# Variables
SPARK_NAMESPACE="spark"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
JOB_NAME="spark-delta-nfs-${TIMESTAMP}"
SPARK_IMAGE="aladdin7/spark-delta:3.5.0"
PVC_NAME="pvc-spark"  # Remplacez par votre PVC

# Vérifier que le fichier existe
echo "Vérification de l'existence du fichier delta_jobs.py..."
kubectl run temp-check --rm -i --restart=Never --image=busybox:1.35 \
  --namespace=${SPARK_NAMESPACE} \
  --overrides="{
    \"spec\": {
      \"containers\": [{
        \"name\": \"temp-check\",
        \"image\": \"busybox:1.35\",
        \"command\": [\"ls\", \"-la\", \"/data/jobs/tests/delta_jobs.py\"],
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
  }" -- ls -la /data/jobs/tests/delta_jobs.py

if [ $? -ne 0 ]; then
    echo "Erreur: Le fichier delta_jobs.py n'existe pas!"
    exit 1
fi

# Créer et soumettre le job Spark
echo "Soumission du job Spark..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: spark-submit-${JOB_NAME}
  namespace: ${SPARK_NAMESPACE}
  labels:
    app: spark-delta-nfs
spec:
  serviceAccountName: spark
  containers:
  - name: spark-submit
    image: ${SPARK_IMAGE}
    command: ["/opt/bitnami/spark/bin/spark-submit"]
    args:
      - "--master=k8s://https://kubernetes.default.svc"
      - "--name=${JOB_NAME}"
      - "--conf=spark.kubernetes.file.upload.path=file:///data"
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
      - "--conf=spark.executor.memory=1g"
      - "--conf=spark.executor.cores=1"
      - "--conf=spark.driver.memory=1g"
      - "--deploy-mode=cluster"
      - "--verbose"
      - "/data/jobs/tests/delta_jobs.py"
    volumeMounts:
    - name: data-volume
      mountPath: /data
    resources:
      requests:
        memory: "250Mi"
        cpu: "100m"
      limits:
        memory: "500Mi"
        cpu: "200m"
  volumes:
  - name: data-volume
    persistentVolumeClaim:
      claimName: ${PVC_NAME}
  restartPolicy: Never
EOF

# Attendre et surveiller le job
echo "Surveillance du job..."
kubectl wait --for=condition=Ready pod/spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE} --timeout=300s

# Suivre les logs
echo "Affichage des logs..."
kubectl logs -f spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}

# Afficher le statut final
echo "Statut final:"
kubectl get pod spark-submit-${JOB_NAME} -n ${SPARK_NAMESPACE}

# Afficher les pods Spark créés
echo "Pods Spark créés:"
kubectl get pods -n ${SPARK_NAMESPACE} -l spark-app-name=${JOB_NAME}

# Nettoyer (optionnel - décommentez si vous voulez nettoyer automat
