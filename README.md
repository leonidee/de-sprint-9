

# Build image and push to registry

```shell
export KUBECONFIG=$HOME/.kube/config
```

```shell
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/stg-collector-app:v20230826-r1 -f ./docker/stg-collector-app/Dockerfile .
```

```shell
docker images | grep "stg-collector-app" | grep "v20230826-r1"
```

Push image to registry:

```shell
docker push cr.yandex/$YC_REGISTRY_ID/stg-collector-app:v20230826-r1
```

# Deploy helm chart to kubernetes 

```shell
cd /apps/stg-collector-app/helm
```

```shell
helm upgrade --install --atomic stg-collector-app . -n c12-leonid-grishenkov 
```
