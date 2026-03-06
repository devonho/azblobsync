ENV_FILE="../.env"
if [ ! -f "$ENV_FILE" ]; then
	echo "Error: $ENV_FILE not found. Please create it before running this script." >&2
	exit 1
fi

export $(cat $ENV_FILE | xargs)
az containerapp logs show --name $CONTAINER_APP_NAME -g $RESOURCE_GROUP --follow true --tail 50 --format text --type console