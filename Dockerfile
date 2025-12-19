FROM astrocrpublic.azurecr.io/runtime:3.1-7

# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-bigquery==1.10.3 && deactivate