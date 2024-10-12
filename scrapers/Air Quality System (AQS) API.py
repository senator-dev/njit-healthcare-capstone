# Databricks notebook source
import requests

# COMMAND ----------

email = "the.senate.dev@proton.me"

# COMMAND ----------

requests.get(f"https://aqs.epa.gov/data/api/signup?email={email}")

# COMMAND ----------


