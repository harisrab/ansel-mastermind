# airbyte-backend

For deployment follow the following procedures:
1. 
```shell
# Login and setup
az login
az acr update -n ansel --admin-enabled true
az acr login --name ansel

# Test it locally
docker run --detach --publish 3100:3100 ansel-mastermind 

# Tag it up and push it.
docker tag ansel-mastermind:latest ansel.azurecr.io/ansel-mastermind:latest
docker push ansel.azurecr.io/ansel-mastermind:latest
```