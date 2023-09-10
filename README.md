

# Test app

```shell
docker compose up stg-collector-app --build
```


# Build image and push to registry

```shell
export KUBECONFIG=$HOME/.kube/config
```

```shell
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/stg-collector-app:v20230826-r1.1 -f ./apps/stg-collector-app/Dockerfile .
```

```shell
docker images | grep "stg-collector-app" 
```

Push image to registry:

```shell
docker push cr.yandex/$YC_REGISTRY_ID/stg-collector-app:v20230826-r1.1
```

# Deploy helm chart to kubernetes 

```shell
cd /apps/stg-collector-app/helm
```

```shell
helm upgrade --install --atomic stg-collector-app . -n c12-leonid-grishenkov 
```


---


```shell
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/stg-collector-app:v20230909-r1.0 -f ./apps/stg-collector-app/Dockerfile .
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/dds-collector-app:v20230909-r1.0 -f ./apps/dds-collector-app/Dockerfile .
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/cdm-collector-app:v20230909-r1.0 -f ./apps/cdm-collector-app/Dockerfile .


docker push cr.yandex/$YC_REGISTRY_ID/cdm-collector-app:v20230909-r1.0

helm upgrade --install --atomic cdm-collector-app . -n c12-leonid-grishenkov 
```