# server.py

from mcp.server.fastmcp import FastMCP
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from dotenv import load_dotenv
import json
import os


# Import the Watsonx.data SDK. (Adjust the import if your SDK organization is different.)
from ibm_watsonxdata import watsonx_data_v2

load_dotenv()

api_key = os.getenv("IBM_CLOUD_IAM_APIKEY")

# Instantiate the MCP server.
mcp = FastMCP("WatsonxdataWrapper")

# Create a global Watsonx.data client instance.
# This assumes that the Watsonx.data SDK supports a factory method (e.g. new_instance()).
try:
    authenticator = IAMAuthenticator(api_key)
    client = watsonx_data_v2.WatsonxDataV2(authenticator=authenticator)
except Exception as init_error:
    # If initialization fails, log or handle the error appropriately.
    client = None
    print(f"Error initializing WatsonxDataV2 client: {init_error}")


@mcp.tool()
def get_sdk_version() -> str:
    """
    Retrieves the version of the Watsonx.data Python SDK.
    
    Returns:
        A string with the SDK version.
    """
    return getattr(watsonx_data_v2, "__version__", "unknown")


@mcp.tool()
def list_buckets() -> dict:
    """
    Lists all buckets available in your Watsonx.data instance.
    
    Returns:
        A dictionary with the bucket list details.
    """
    try:
        response = client.list_buckets()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_bucket_details(bucket_id: str) -> dict:
    """
    Retrieves details for a specific bucket.
    
    Args:
        bucket_id: The identifier of the bucket.
        
    Returns:
        A dictionary with bucket detail information.
    """
    try:
        response = client.get_bucket(bucket_id=bucket_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def create_bucket(bucket_data_json: str) -> dict:
    """
    Creates a new bucket with the specified parameters.
    
    Args:
        bucket_data_json: A JSON string containing the bucket parameters.
        
    Returns:
        A dictionary with the response from the create bucket operation.
    """
    try:
        bucket_data = json.loads(bucket_data_json)
        response = client.create_bucket(body=bucket_data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def delete_bucket(bucket_id: str) -> dict:
    """
    Deletes a bucket identified by its bucket ID.
    
    Args:
        bucket_id: The identifier of the bucket to delete.
        
    Returns:
        A dictionary with the result of the delete operation.
    """
    try:
        response = client.delete_bucket(bucket_id=bucket_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_databases() -> dict:
    """
    Retrieves a list of all databases in your Watsonx.data instance.
    
    Returns:
        A dictionary containing the database list.
    """
    try:
        response = client.list_databases()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_database_details(database_id: str) -> dict:
    """
    Retrieves details for a specific database.
    
    Args:
        database_id: The identifier of the database.
        
    Returns:
        A dictionary with database detail information.
    """
    try:
        response = client.get_database(database_id=database_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def create_database(database_data_json: str) -> dict:
    """
    Creates a new database with the provided parameters.
    
    Args:
        database_data_json: A JSON string containing database configuration details.
        
    Returns:
        A dictionary with the response from the create database operation.
    """
    try:
        database_data = json.loads(database_data_json)
        response = client.create_database(body=database_data)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def delete_database(database_id: str) -> dict:
    """
    Deletes a database identified by its database ID.
    
    Args:
        database_id: The identifier of the database to delete.
        
    Returns:
        A dictionary with the result of the delete operation.
    """
    try:
        response = client.delete_database(database_id=database_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_schemas() -> dict:
    """
    Lists all schemas available.
    
    Returns:
        A dictionary with the list of schemas.
    """
    try:
        response = client.list_schemas()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_schema_details(schema_id: str) -> dict:
    """
    Retrieves detailed information for a specific schema.
    
    Args:
        schema_id: The identifier of the schema.
        
    Returns:
        A dictionary with schema details.
    """
    try:
        response = client.get_schema(schema_id=schema_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_tables() -> dict:
    """
    Lists all tables.
    
    Returns:
        A dictionary containing the list of tables.
    """
    try:
        response = client.list_tables()
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_table_details(table_id: str) -> dict:
    """
    Retrieves details for a specific table.
    
    Args:
        table_id: The identifier of the table.
        
    Returns:
        A dictionary with table details.
    """
    try:
        response = client.get_table(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def delete_table(table_id: str) -> dict:
    """
    Deletes a table by its identifier.
    
    Args:
        table_id: The identifier of the table to delete.
        
    Returns:
        A dictionary with the result of the deletion.
    """
    try:
        response = client.delete_table(table_id=table_id)
        return response.get_result()
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    mcp.run()

