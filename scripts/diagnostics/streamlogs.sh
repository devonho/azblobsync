export $(cat .env | xargs)
az containerapp logs show --name $CONTAINER_APP_NAME -g $RESOURCE_GROUP --follow true --tail 50 --format text --type console