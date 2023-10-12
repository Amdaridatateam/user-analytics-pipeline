gcloud container clusters get-credentials $(terraform output -raw k8s_cluster_name) --region $(terraform output -raw k8s_cluster_location)
kubectl create ns nfs
kubectl apply -n nfs -f nfs/nfs-server.yaml
export NFS_SERVER=$(kubectl get svc/nfs-server -n nfs -o jsonpath="{.spec.clusterIP}")
kubectl create ns storage

helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner -n storage --set nfs.server=$NFS_SERVER --set nfs.path=/