ENV_FILE="../.env"
if [ ! -f "$ENV_FILE" ]; then
	echo "Error: $ENV_FILE not found. Please create it before running this script." >&2
	exit 1
fi

export $(cat $ENV_FILE | xargs)

az containerapp up --name $CONTAINER_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --environment $CONTAINER_APP_ENVIRONMENT \
    --image $CONTAINER_APP_IMAGE \
    --query properties.configuration.ingress.fqdn

az containerapp ingress disable --name $CONTAINER_APP_NAME --resource-group $RESOURCE_GROUP

