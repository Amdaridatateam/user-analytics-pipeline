$clusterName = (terraform output -raw k8s_cluster_name)
$clusterLocation = (terraform output -raw k8s_cluster_location)

gcloud container clusters get-credentials $clusterName --region $clusterLocation

kubectl create ns nfs
kubectl apply -n nfs -f ../../nfs/nfs-server.yaml

$env:NFS_SERVER = (kubectl get svc/nfs-server -n nfs -o jsonpath="{.spec.clusterIP}")

kubectl create namespace storage
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/

helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner --namespace storage --set nfs.server=$env:NFS_SERVER --set nfs.path=/

kubectl create namespace airflow

helm repo add apache-airflow https://airflow.apache.org

helm upgrade --install airflow -f airflow-values.yaml apache-airflow/airflow --namespace airflow