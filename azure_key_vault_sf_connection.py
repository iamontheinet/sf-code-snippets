### Pre-reqs: 
# snowflake.snowpark
# pip install azure-keyvault-secrets azure-identity azure-cli
# Log into your Azure account via Azure CLI (i.e. az login) 
###

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Load Snowflake connection details from Azure Key Vault
def get_azure_sf_connection_details(vault_url):
    
    # Create DefaultAzureCredential credential client 
    credential = DefaultAzureCredential(
        exclude_azurecli_credential=False,
        exclude_environment_credential=True,
        exclude_managed_identity_credential=True,
        exclude_powershell_credential=True,
        exclude_visual_studio_code_credential=True,
        exclude_shared_token_cache_credential=True,
        exclude_interactive_browser_credential=True
    )
    
    # Get secret values(s) based on the passed in Key Vault URL
    secret_client = SecretClient(vault_url=vault_url, credential=credential)
    
    connection_params = {
      "account"       : secret_client.get_secret('account').value,
      "user"          : secret_client.get_secret('user').value,
      "password"      : secret_client.get_secret('password').value,
      "role"          : secret_client.get_secret('role').value,
      "warehouse"     : secret_client.get_secret('warehouse').value,
      "database"      : secret_client.get_secret('database').value,
      "schema"        : secret_client.get_secret('schema').value
    }

    return connection_params

# Create Snowflake Session object
connection_parameters = get_azure_sf_connection_details('YOUR_AZURE_VAULT_URL')
session = Session.builder.configs(connection_parameters).create()
print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
