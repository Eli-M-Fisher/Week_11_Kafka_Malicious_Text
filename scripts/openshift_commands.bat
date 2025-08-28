oc version 

oc login ....> 

REM delete and clean old project
oc delete project eliyahu-dev


REM 1. setting up the project
oc new-project malicious-text-system

REM building and pushing images to dockerhub
docker build -t elif2/retriever:latest ./services/retriever
docker push elif2/retriever:latest
docker build -t elif2/preprocessor:latest ./services/preprocessor
docker push elif2/preprocessor:latest

docker build -t elif2/enricher:latest ./services/enricher
docker push elif2/enricher:latest

docker build -t elif2/persister:latest ./services/persister
docker push elif2/persister:latest

docker build -t elif2/dataretrieval:latest ./services/dataretrieval
docker push elif2/dataretrieval:latest


oc apply -f infra/configmap.yaml
oc apply -f infra/mongo-secret.yaml



oc edit secret mongo-secret


REM 2. setting up mongodb
oc apply -f infra/mongo-deployment.yaml


oc get pods
oc get svc

oc apply -f infra/zookeeper-deployment.yaml
oc apply -f infra/kafka-deployment.yaml

oc get pods
oc get svc

REM 6. and deploying the microservices

oc apply -f infra/retriever-deployment.yaml
oc apply -f infra/preprocessor-deployment.yaml
oc apply -f infra/enricher-deployment.yaml
oc apply -f infra/persister-deployment.yaml
oc apply -f infra/dataretrieval-deployment.yaml

oc expose svc/dataretrieval

oc get routes

REM and I got a public URL:

http://dataretrieval-malicious-text-system.apps.<cluster-domain>/

REM 8. Tests if needed:

REM Check that the pods are working:

oc get pods -o wide

REM Checking retriever logs:

oc logs deployment/retriever