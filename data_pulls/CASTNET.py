# Databricks notebook source
access_key = dbutils.secrets.get(scope="aws", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope="aws", key = "aws-secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "njit-healthcare-bucket"
mount_name = "<mount-name>"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------


