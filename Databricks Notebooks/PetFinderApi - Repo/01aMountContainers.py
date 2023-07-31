# Databricks notebook source
# MAGIC %md
# MAGIC # Define function for mounting container

# COMMAND ----------

# Define function for mounting containers
def mount_adls(storage_account_name, container_name):
    #Get secrets from key vaults
    client_ID           = dbutils.secrets.get(scope = 'SummerInterview2023',  key = 'sc-si-app-client-id')
    tenant_ID           = dbutils.secrets.get(scope = 'SummerInterview2023',  key = 'sc-si-app-tenant-id')
    client_secret       = dbutils.secrets.get(scope = 'SummerInterview2023',  key = 'sc-si-app-client-secret')

    #Set Spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_ID,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_ID}/oauth2/token"}
    
    #Umount if previously mounted
    if any (mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    #Mount the storage account containers
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC # Call function for mounting containers on raw, processed and presentation

# COMMAND ----------

#Call function for mounting containers
mount_adls('dlsi2023', 'raw')
mount_adls('dlsi2023', 'processed')
mount_adls('dlsi2023', 'presentation')

# COMMAND ----------

mount_adls('dlsi2023', 'config')

# COMMAND ----------

mount_adls('dlsi2023', 'logtables')
