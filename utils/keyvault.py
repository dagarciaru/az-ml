from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from os import environ

key_vault_uri = environ.get('KEYVAULT_URL')

credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_uri, credential=credential)

def get_secret(secret):
    retrieved_secret = client.get_secret(secret)
    return retrieved_secret.value
