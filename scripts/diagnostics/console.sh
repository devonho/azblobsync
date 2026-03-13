ENV_FILE="../.env"
if [ ! -f "$ENV_FILE" ]; then
	echo "Error: $ENV_FILE not found. Please create it before running this script." >&2
	exit 1
fi

export $(cat $ENV_FILE | xargs)

az containerapp exec \
  --name $CONTAINER_APP_NAME \
  --resource-group $RESOURCE_GROUP