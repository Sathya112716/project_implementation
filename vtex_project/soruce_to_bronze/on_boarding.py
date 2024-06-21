# Databricks notebook source
storage_account_name = "projectimplementation"
storage_account_access_key = f"zq5VRBwbBMHPdGeA9pyAEeaZjmEsnUyNEnrnlyRdr7hDb2YjFEK+b1U96GCruuzeuAKR9pjQklnV+AStYNFPgQ=="
container_name = "data"
 
# Create the DBFS mount point
mount_point = f"/mnt/{container_name}"
 
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = mount_point,
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/data/vtex_test_data (1).json")
display(df)

# COMMAND ----------

