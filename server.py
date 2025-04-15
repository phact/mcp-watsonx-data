# server.py

from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
import json
import os

# Import the Watsonx.data SDK module.
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watsonxdata import watsonx_data_v2

# Load environment variables from .env
load_dotenv()

api_key = os.getenv("IBM_CLOUD_IAM_APIKEY")
if not api_key:
    raise ValueError("IBM_CLOUD_IAM_APIKEY environment variable not set.")

service_url = os.getenv("IBM_CLOUD_IAM_URL")
if not service_url:
    raise ValueError("IBM_CLOUD_IAM_URL environment variable not set.")



# Instantiate the MCP server.
mcp = FastMCP("WatsonxdataWrapper")

# Create a global Watsonx.data client instance.
try:
    authenticator = IAMAuthenticator(api_key)
    # Use the factory or new_instance method if available.
    client = watsonx_data_v2.WatsonxDataV2(authenticator=authenticator)
    client.set_service_url(service_url)
#    client.set_disable_ssl_verification(True)
#    test = watsonx_data_v2.WatsonxDataV2.new_instance()
#    test.disable_ssl_verification = True
#    response = test.list_bucket_registrations()
#    print(response)

except Exception as init_error:
    client = None
    print(f"Error initializing WatsonxDataV2 client: {init_error}")


# =============================================================================
# Bucket Registration & Storage Operations
# =============================================================================

@mcp.tool()
def list_bucket_registrations() -> dict:
    try:
        response = client.list_bucket_registrations()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_bucket_registration(bucket_reg_data_json: str) -> dict:
    try:
        data = json.loads(bucket_reg_data_json)
        response = client.create_bucket_registration(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_bucket_registration(bucket_reg_id: str) -> dict:
    try:
        response = client.get_bucket_registration(bucket_reg_id=bucket_reg_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_bucket_registration(bucket_reg_id: str) -> dict:
    try:
        response = client.delete_bucket_registration(bucket_reg_id=bucket_reg_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_bucket_registration(bucket_reg_id: str, bucket_reg_data_json: str) -> dict:
    try:
        data = json.loads(bucket_reg_data_json)
        response = client.update_bucket_registration(bucket_reg_id=bucket_reg_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_activate_bucket(bucket_reg_id: str) -> dict:
    try:
        response = client.create_activate_bucket(bucket_reg_id=bucket_reg_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_deactivate_bucket(bucket_reg_id: str) -> dict:
    try:
        response = client.delete_deactivate_bucket(bucket_reg_id=bucket_reg_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_bucket_objects(bucket_reg_id: str) -> dict:
    try:
        response = client.list_bucket_objects(bucket_reg_id=bucket_reg_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_bucket_object_properties(bucket_reg_id: str, object_path: str) -> dict:
    try:
        response = client.get_bucket_object_properties(bucket_reg_id=bucket_reg_id, object_path=object_path)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_hdfs_storage(hdfs_data_json: str) -> dict:
    try:
        data = json.loads(hdfs_data_json)
        response = client.create_hdfs_storage(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Database Registration Operations
# =============================================================================

@mcp.tool()
def list_database_registrations() -> dict:
    try:
        response = client.list_database_registrations()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_database_registration(db_reg_data_json: str) -> dict:
    try:
        data = json.loads(db_reg_data_json)
        response = client.create_database_registration(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_database(database_id: str) -> dict:
    try:
        response = client.get_database(database_id=database_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_database_catalog(catalog_id: str) -> dict:
    try:
        response = client.delete_database_catalog(catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_database(database_id: str, db_data_json: str) -> dict:
    try:
        data = json.loads(db_data_json)
        response = client.update_database(database_id=database_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Driver Registration Operations
# =============================================================================

#@mcp.tool()
#def list_driver_registration() -> dict:
#    try:
#        response = client.list_driver_registration()
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_driver_registration(driver_data_json: str) -> dict:
#    try:
#        data = json.loads(driver_data_json)
#        response = client.create_driver_registration(body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def delete_driver_registration(driver_id: str) -> dict:
#    try:
#        response = client.delete_driver_registration(driver_id=driver_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def delete_driver_engines(driver_id: str) -> dict:
#    try:
#        response = client.delete_driver_engines(driver_id=driver_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def update_driver_engines(driver_id: str, engines_data_json: str) -> dict:
#    try:
#        data = json.loads(engines_data_json)
#        response = client.update_driver_engines(driver_id=driver_id, body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}


# =============================================================================
# Other Engine Operations
# =============================================================================

#@mcp.tool()
#def list_other_engines() -> dict:
#    try:
#        response = client.list_other_engines()
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_other_engine(engine_data_json: str) -> dict:
#    try:
#        data = json.loads(engine_data_json)
#        response = client.create_other_engine(body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def delete_other_engine(engine_id: str) -> dict:
#    try:
#        response = client.delete_other_engine(engine_id=engine_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}


# =============================================================================
# Integration Operations
# =============================================================================

#@mcp.tool()
#def list_all_integrations() -> dict:
#    try:
#        response = client.list_all_integrations()
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_integration(integration_data_json: str) -> dict:
#    try:
#        data = json.loads(integration_data_json)
#        response = client.create_integration(body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def get_integrations() -> dict:
#    try:
#        response = client.get_integrations()
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def delete_integration(integration_id: str) -> dict:
#    try:
#        response = client.delete_integration(integration_id=integration_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def update_integration(integration_id: str, integration_data_json: str) -> dict:
#    try:
#        data = json.loads(integration_data_json)
#        response = client.update_integration(integration_id=integration_id, body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}


# =============================================================================
# DB2 Engine Operations
# =============================================================================

@mcp.tool()
def list_db2_engines() -> dict:
    try:
        response = client.list_db2_engines()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_db2_engine(db2_data_json: str) -> dict:
    try:
        data = json.loads(db2_data_json)
        response = client.create_db2_engine(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_db2_engine(db2_engine_id: str) -> dict:
    try:
        response = client.delete_db2_engine(db2_engine_id=db2_engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_db2_engine(db2_engine_id: str, db2_data_json: str) -> dict:
    try:
        data = json.loads(db2_data_json)
        response = client.update_db2_engine(db2_engine_id=db2_engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Netezza Engine Operations
# =============================================================================

#@mcp.tool()
#def list_netezza_engines() -> dict:
#    try:
#        response = client.list_netezza_engines()
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_netezza_engine(netezza_data_json: str) -> dict:
#    try:
#        data = json.loads(netezza_data_json)
#        response = client.create_netezza_engine(body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def delete_netezza_engine(netezza_engine_id: str) -> dict:
#    try:
#        response = client.delete_netezza_engine(netezza_engine_id=netezza_engine_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def update_netezza_engine(netezza_engine_id: str, netezza_data_json: str) -> dict:
#    try:
#        data = json.loads(netezza_data_json)
#        response = client.update_netezza_engine(netezza_engine_id=netezza_engine_id, body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#

# =============================================================================
# Query Execution Operations
# =============================================================================

@mcp.tool()
def create_execute_query(query_data_json: str) -> dict:
    try:
        data = json.loads(query_data_json)
        response = client.create_execute_query(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Instance / Service Details Operations
# =============================================================================

@mcp.tool()
def list_instance_details() -> dict:
    try:
        response = client.list_instance_details()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_instance_service_details() -> dict:
    try:
        response = client.list_instance_service_details()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_services_details() -> dict:
    try:
        response = client.get_services_details()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_service_detail(service_id: str) -> dict:
    try:
        response = client.get_service_detail(service_id=service_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Prestissimo Engine Operations
# =============================================================================

@mcp.tool()
def list_prestissimo_engines() -> dict:
    try:
        response = client.list_prestissimo_engines()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_prestissimo_engine(engine_data_json: str) -> dict:
    try:
        data = json.loads(engine_data_json)
        response = client.create_prestissimo_engine(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_prestissimo_engine(engine_id: str) -> dict:
    try:
        response = client.get_prestissimo_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_prestissimo_engine(engine_id: str) -> dict:
    try:
        response = client.delete_prestissimo_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_prestissimo_engine(engine_id: str, engine_data_json: str) -> dict:
    try:
        data = json.loads(engine_data_json)
        response = client.update_prestissimo_engine(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_prestissimo_engine_catalogs(engine_id: str) -> dict:
    try:
        response = client.list_prestissimo_engine_catalogs(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_prestissimo_engine_catalogs(engine_id: str, catalog_data_json: str) -> dict:
    try:
        data = json.loads(catalog_data_json)
        response = client.create_prestissimo_engine_catalogs(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_prestissimo_engine_catalogs(engine_id: str, catalog_id: str) -> dict:
    try:
        response = client.delete_prestissimo_engine_catalogs(engine_id=engine_id, catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_prestissimo_engine_catalog(engine_id: str, catalog_id: str) -> dict:
    try:
        response = client.get_prestissimo_engine_catalog(engine_id=engine_id, catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def pause_prestissimo_engine(engine_id: str) -> dict:
    try:
        response = client.pause_prestissimo_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def run_prestissimo_explain_statement(statement: str) -> dict:
    try:
        response = client.run_prestissimo_explain_statement(sql_string=statement)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def run_prestissimo_explain_analyze_statement(statement: str) -> dict:
    try:
        response = client.run_prestissimo_explain_analyze_statement(sql_string=statement)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def restart_prestissimo_engine(engine_id: str) -> dict:
    try:
        response = client.restart_prestissimo_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def resume_prestissimo_engine(engine_id: str) -> dict:
    try:
        response = client.resume_prestissimo_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def scale_prestissimo_engine(engine_id: str, scale_data_json: str) -> dict:
    try:
        data = json.loads(scale_data_json)
        response = client.scale_prestissimo_engine(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Presto Engine Operations
# =============================================================================

@mcp.tool()
def list_presto_engines() -> dict:
    try:
        response = client.list_presto_engines()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_presto_engine(engine_data_json: str) -> dict:
    try:
        data = json.loads(engine_data_json)
        response = client.create_presto_engine(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_presto_engine(engine_id: str) -> dict:
    try:
        response = client.get_presto_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_engine(engine_id: str) -> dict:
    try:
        response = client.delete_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_presto_engine(engine_id: str, engine_data_json: str) -> dict:
    try:
        data = json.loads(engine_data_json)
        response = client.update_presto_engine(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_presto_engine_catalogs(engine_id: str) -> dict:
    try:
        response = client.list_presto_engine_catalogs(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_presto_engine_catalogs(engine_id: str, catalog_data_json: str) -> dict:
    try:
        data = json.loads(catalog_data_json)
        response = client.create_presto_engine_catalogs(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_presto_engine_catalogs(engine_id: str, catalog_id: str) -> dict:
    try:
        response = client.delete_presto_engine_catalogs(engine_id=engine_id, catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_presto_engine_catalog(engine_id: str, catalog_id: str) -> dict:
    try:
        response = client.get_presto_engine_catalog(engine_id=engine_id, catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def pause_presto_engine(engine_id: str) -> dict:
    try:
        response = client.pause_presto_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def run_explain_statement(query_string: str) -> dict:
    try:
        response = client.run_explain_statement(sql_string=query_string)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def run_explain_analyze_statement(query_string: str) -> dict:
    try:
        response = client.run_explain_analyze_statement(sql_string=query_string)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def restart_presto_engine(engine_id: str) -> dict:
    try:
        response = client.restart_presto_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def resume_presto_engine(engine_id: str) -> dict:
    try:
        response = client.resume_presto_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def scale_presto_engine(engine_id: str, scale_data_json: str) -> dict:
    try:
        data = json.loads(scale_data_json)
        response = client.scale_presto_engine(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# SAL (Semantic Automation Layer) Operations
# =============================================================================

@mcp.tool()
def get_sal_integration() -> dict:
    try:
        response = client.get_sal_integration()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_sal_integration(sal_data_json: str) -> dict:
    try:
        data = json.loads(sal_data_json)
        response = client.create_sal_integration(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_sal_integration(integration_id: str) -> dict:
    try:
        response = client.delete_sal_integration(integration_id=integration_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_sal_integration(integration_id: str, sal_data_json: str) -> dict:
    try:
        data = json.loads(sal_data_json)
        response = client.update_sal_integration(integration_id=integration_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_sal_integration_enrichment(enrichment_data_json: str) -> dict:
    try:
        data = json.loads(enrichment_data_json)
        response = client.create_sal_integration_enrichment(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_assets() -> dict:
    try:
        response = client.get_sal_integration_enrichment_assets()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_data_asset() -> dict:
    try:
        response = client.get_sal_integration_enrichment_data_asset()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_job_run_logs(job_id: str) -> dict:
    try:
        response = client.get_sal_integration_enrichment_job_run_logs(job_id=job_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_job_runs(job_id: str) -> dict:
    try:
        response = client.get_sal_integration_enrichment_job_runs(job_id=job_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_jobs() -> dict:
    try:
        response = client.get_sal_integration_enrichment_jobs()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_glossary_terms() -> dict:
    try:
        response = client.get_sal_integration_glossary_terms()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_mappings() -> dict:
    try:
        response = client.get_sal_integration_mappings()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_global_settings() -> dict:
    try:
        response = client.get_sal_integration_enrichment_global_settings()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_sal_integration_enrichment_global_settings(settings_json: str) -> dict:
    try:
        data = json.loads(settings_json)
        response = client.create_sal_integration_enrichment_global_settings(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_enrichment_settings() -> dict:
    try:
        response = client.get_sal_integration_enrichment_settings()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_sal_integration_enrichment_settings(settings_json: str) -> dict:
    try:
        data = json.loads(settings_json)
        response = client.create_sal_integration_enrichment_settings(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_sal_integration_upload_glossary(glossary_json: str) -> dict:
    try:
        data = json.loads(glossary_json)
        response = client.create_sal_integration_upload_glossary(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sal_integration_upload_glossary_status(process_id: str) -> dict:
    try:
        response = client.get_sal_integration_upload_glossary_status(process_id=process_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Spark Engine Operations
# =============================================================================

@mcp.tool()
def list_spark_engines() -> dict:
    try:
        response = client.list_spark_engines()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_spark_engine(spark_data_json: str) -> dict:
    try:
        data = json.loads(spark_data_json)
        response = client.create_spark_engine(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_spark_engine(engine_id: str) -> dict:
    try:
        response = client.get_spark_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_spark_engine(engine_id: str) -> dict:
    try:
        response = client.delete_spark_engine(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_spark_engine(engine_id: str, spark_data_json: str) -> dict:
    try:
        data = json.loads(spark_data_json)
        response = client.update_spark_engine(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_spark_engine_applications() -> dict:
    try:
        response = client.list_spark_engine_applications()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_spark_engine_application(application_data_json: str) -> dict:
    try:
        data = json.loads(application_data_json)
        response = client.create_spark_engine_application(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_spark_engine_applications(app_id: str) -> dict:
    try:
        response = client.delete_spark_engine_applications(app_id=app_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_spark_engine_application_status(app_id: str) -> dict:
    try:
        response = client.get_spark_engine_application_status(app_id=app_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_spark_engine_catalogs(engine_id: str) -> dict:
    try:
        response = client.list_spark_engine_catalogs(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_spark_engine_catalogs(engine_id: str, catalog_data_json: str) -> dict:
    try:
        data = json.loads(catalog_data_json)
        response = client.create_spark_engine_catalogs(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_spark_engine_catalogs(engine_id: str, catalog_id: str) -> dict:
    try:
        response = client.delete_spark_engine_catalogs(engine_id=engine_id, catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_spark_engine_catalog(engine_id: str, catalog_id: str) -> dict:
    try:
        response = client.get_spark_engine_catalog(engine_id=engine_id, catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_spark_engine_history_server(engine_id: str) -> dict:
    try:
        response = client.get_spark_engine_history_server(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def start_spark_engine_history_server(engine_id: str) -> dict:
    try:
        response = client.start_spark_engine_history_server(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_spark_engine_history_server(engine_id: str) -> dict:
    try:
        response = client.delete_spark_engine_history_server(engine_id=engine_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def pause_spark_engine() -> dict:
    try:
        response = client.pause_spark_engine()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def resume_spark_engine() -> dict:
    try:
        response = client.resume_spark_engine()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def scale_spark_engine(engine_id: str, scale_data_json: str) -> dict:
    try:
        data = json.loads(scale_data_json)
        response = client.scale_spark_engine(engine_id=engine_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_spark_versions() -> dict:
    try:
        response = client.list_spark_versions()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Catalog, Schema, Table, Column, Snapshot, Sync Operations
# =============================================================================

@mcp.tool()
def list_catalogs() -> dict:
    try:
        response = client.list_catalogs()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_catalog(catalog_id: str) -> dict:
    try:
        response = client.get_catalog(catalog_id=catalog_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_schemas() -> dict:
    try:
        response = client.list_schemas()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_schema(schema_data_json: str) -> dict:
    try:
        data = json.loads(schema_data_json)
        response = client.create_schema(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_schema(schema_id: str) -> dict:
    try:
        response = client.delete_schema(schema_id=schema_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_tables() -> dict:
    try:
        response = client.list_tables()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_table(table_id: str) -> dict:
    try:
        response = client.get_table(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_table(table_id: str) -> dict:
    try:
        response = client.delete_table(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_table(table_id: str, table_data_json: str) -> dict:
    try:
        data = json.loads(table_data_json)
        response = client.update_table(table_id=table_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_columns(table_id: str) -> dict:
    try:
        response = client.list_columns(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_columns(table_id: str, columns_data_json: str) -> dict:
    try:
        data = json.loads(columns_data_json)
        response = client.create_columns(table_id=table_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_column(table_id: str, column_id: str) -> dict:
    try:
        response = client.delete_column(table_id=table_id, column_id=column_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_column(table_id: str, column_id: str, column_data_json: str) -> dict:
    try:
        data = json.loads(column_data_json)
        response = client.update_column(table_id=table_id, column_id=column_id, body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_table_snapshots(table_id: str) -> dict:
    try:
        response = client.list_table_snapshots(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def rollback_table(table_id: str, snapshot_id: str) -> dict:
    try:
        response = client.rollback_table(table_id=table_id, snapshot_id=snapshot_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def update_sync_catalog(sync_data_json: str) -> dict:
    try:
        data = json.loads(sync_data_json)
        response = client.update_sync_catalog(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Milvus Operations
# =============================================================================

#@mcp.tool()
#def list_milvus_services() -> dict:
#    try:
#        response = client.list_milvus_services()
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_milvus_service(milvus_data_json: str) -> dict:
#    try:
#        data = json.loads(milvus_data_json)
#        response = client.create_milvus_service(body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def get_milvus_service(service_id: str) -> dict:
#    try:
#        response = client.get_milvus_service(service_id=service_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def delete_milvus_service(service_id: str) -> dict:
#    try:
#        response = client.delete_milvus_service(service_id=service_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def update_milvus_service(service_id: str, milvus_data_json: str) -> dict:
#    try:
#        data = json.loads(milvus_data_json)
#        response = client.update_milvus_service(service_id=service_id, body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def update_milvus_service_bucket(service_id: str, bucket_data_json: str) -> dict:
#    try:
#        data = json.loads(bucket_data_json)
#        response = client.update_milvus_service_bucket(service_id=service_id, body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def list_milvus_service_databases(service_id: str) -> dict:
#    try:
#        response = client.list_milvus_service_databases(service_id=service_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def list_milvus_database_collections(database_id: str) -> dict:
#    try:
#        response = client.list_milvus_database_collections(database_id=database_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_milvus_service_pause(service_id: str) -> dict:
#    try:
#        response = client.create_milvus_service_pause(service_id=service_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_milvus_service_resume(service_id: str) -> dict:
#    try:
#        response = client.create_milvus_service_resume(service_id=service_id)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}
#
#@mcp.tool()
#def create_milvus_service_scale(service_id: str, scale_data_json: str) -> dict:
#    try:
#        data = json.loads(scale_data_json)
#        response = client.create_milvus_service_scale(service_id=service_id, body=data)
#        return response.get_result()
#    except Exception as e:
#        return {"error": str(e)}


# =============================================================================
# Ingestion Operations
# =============================================================================

@mcp.tool()
def list_ingestion_jobs() -> dict:
    try:
        response = client.list_ingestion_jobs()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_ingestion_jobs(ingestion_data_json: str) -> dict:
    try:
        data = json.loads(ingestion_data_json)
        response = client.create_ingestion_jobs(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_ingestion_jobs_local_files(ingestion_data_json: str) -> dict:
    try:
        data = json.loads(ingestion_data_json)
        response = client.create_ingestion_jobs_local_files(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_ingestion_job(job_id: str) -> dict:
    try:
        response = client.get_ingestion_job(job_id=job_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def delete_ingestion_jobs(job_id: str) -> dict:
    try:
        response = client.delete_ingestion_jobs(job_id=job_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def create_preview_ingestion_file(preview_data_json: str) -> dict:
    try:
        data = json.loads(preview_data_json)
        response = client.create_preview_ingestion_file(body=data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Miscellaneous Operations
# =============================================================================

@mcp.tool()
def get_endpoints() -> dict:
    try:
        response = client.get_endpoints()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_all_columns() -> dict:
    try:
        response = client.get_all_columns()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_all_schemas() -> dict:
    try:
        response = client.list_all_schemas()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_schema_details_alt(schema_id: str) -> dict:
    try:
        response = client.get_schema_details(schema_id=schema_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def list_all_tables() -> dict:
    try:
        response = client.list_all_tables()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_table_details_alt(table_id: str) -> dict:
    try:
        response = client.get_table_details(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Start the MCP server
# =============================================================================

if __name__ == "__main__":
    mcp.run()

