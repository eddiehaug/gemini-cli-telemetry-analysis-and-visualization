"""
FastAPI backend for Gemini CLI Telemetry deployment automation.
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Gemini CLI Telemetry Deployment API",
    description="API for automating Gemini CLI telemetry deployment to BigQuery",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],  # Vite dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class DeploymentConfig(BaseModel):
    geminiCliProjectId: str = Field(..., description="GCP Project ID for Gemini API inference")
    telemetryProjectId: str = Field(..., description="GCP Project ID for telemetry data collection")
    useSameProjectForGemini: bool = Field(True, description="Whether inference and telemetry use the same project")
    region: str = Field(..., description="GCP Region")
    datasetName: str = Field(..., description="BigQuery dataset name")
    logPrompts: bool = Field(False, description="Whether to log prompts and responses")
    pseudoanonymizePii: bool = Field(False, description="Whether to pseudoanonymize user identifiers")
    network: str = Field("default", description="VPC network name")
    subnetwork: str = Field("default", description="Subnetwork name")
    geminiAuthMethod: str = Field("oauth", description="Gemini CLI authentication method: oauth or vertex-ai")
    geminiRegion: str = Field("us-central1", description="Region for Gemini API calls")


class ApiResponse(BaseModel):
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None
    message: Optional[str] = None


class DependencyCheck(BaseModel):
    name: str
    installed: bool
    version: Optional[str] = None
    path: Optional[str] = None


class PermissionCheck(BaseModel):
    hasAll: bool
    missingRoles: List[str]
    currentRoles: List[str]


class ApiEnablement(BaseModel):
    success: bool
    enabled: List[str]
    failed: List[str]


# Helper functions
async def check_compute_api_enabled(project_id: str) -> bool:
    """
    Check if Compute Engine API is enabled.

    This is a BLOCKING check - if not enabled, we throw an error
    and direct the user to set up their landing zone.

    Args:
        project_id: GCP project ID to check

    Returns:
        True if Compute API is enabled

    Raises:
        Exception if Compute API is not enabled with detailed setup instructions
    """
    from services import api_service

    enabled_apis = await api_service.get_enabled_apis(project_id)

    if "compute.googleapis.com" not in enabled_apis:
        raise Exception(
            f"Compute Engine API is not enabled in project '{project_id}'.\n\n"
            f"This suggests the project landing zone is not configured.\n\n"
            f"Required setup:\n"
            f"1. Enable Compute Engine API in GCP Console\n"
            f"2. Create a custom VPC network (not 'default')\n"
            f"3. Configure subnets in your target region\n"
            f"4. Set up firewall rules as needed\n"
            f"5. Return here to run Bootstrap again\n\n"
            f"Documentation: https://cloud.google.com/vpc/docs/vpc"
        )

    return True


# Health check endpoint
@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "Gemini CLI Telemetry Deployment API"}


# Pre-Authentication: Check auth status (NEW)
@app.post("/api/check-auth-status", response_model=ApiResponse)
async def check_auth_status():
    """
    Check gcloud CLI installation and authentication status.

    This is a non-blocking check that returns current status without
    raising exceptions.

    Returns:
        ApiResponse with:
        - gcloud_installed: bool
        - authenticated: bool
        - account: str | None
        - has_adc: bool
    """
    logger.info("Checking authentication status...")

    try:
        from services import auth_service

        status = await auth_service.check_auth_status()

        logger.info(f"Auth status check complete: gcloud={status['gcloud_installed']}, auth={status['authenticated']}")

        return ApiResponse(
            success=True,
            data=status,
            message="Authentication status retrieved"
        )

    except Exception as e:
        logger.error(f"Auth status check failed: {str(e)}")
        return ApiResponse(success=False, error=str(e))


# Pre-Authentication: Initiate OAuth flow (NEW)
@app.post("/api/authenticate-with-oauth", response_model=ApiResponse)
async def authenticate_with_oauth():
    """
    Initiate OAuth authentication flow and return the auth URL.

    The frontend will open this URL in a new tab for the user to
    complete authentication.

    Returns:
        ApiResponse with:
        - auth_url: str (URL for user to visit)
        - message: str (instructions)
    """
    logger.info("Initiating OAuth authentication flow...")

    try:
        from services import auth_service

        result = await auth_service.initiate_oauth_flow()

        logger.info("OAuth URL generated successfully")

        return ApiResponse(
            success=True,
            data=result,
            message=result.get("message", "Please authenticate in the browser window")
        )

    except Exception as e:
        logger.error(f"OAuth initiation failed: {str(e)}")
        return ApiResponse(success=False, error=str(e))


# Bootstrap: Pre-deployment validation (NEW)
@app.post("/api/bootstrap", response_model=ApiResponse)
async def bootstrap(request: dict):
    """
    Bootstrap the application with 4 validation steps.

    PREREQUISITE: User must be authenticated before running bootstrap.
    This endpoint assumes authentication is already complete.

    Steps:
    1. Verify dependencies (gcloud, gemini, python)
    2. Create gcloud configuration for telemetry project
    3. Check Compute Engine API (BLOCKING)
    4. Enable required APIs (AUTO-ENABLE 10 APIs)
    5. Check VPC networks (BLOCKING - no default-only)

    Args:
        request: Dict containing projectId

    Returns:
        ApiResponse with bootstrap results or error
    """
    project_id = request.get("projectId")
    logger.info(f"Starting bootstrap for project: {project_id}")

    try:
        from services import dependency_service, auth_service, api_service, network_service, gcloud_config_service

        # Step 1: Dependencies
        logger.info("Bootstrap Step 1: Verifying dependencies...")
        deps = await dependency_service.verify_dependencies()

        # Step 2: Get active account (assumes user is already authenticated)
        logger.info("Bootstrap Step 2: Getting active account...")
        account = await auth_service.get_active_account()
        logger.info(f"Using authenticated account: {account}")

        # Step 3: Create gcloud configuration for telemetry project
        logger.info("Bootstrap Step 3: Creating gcloud configuration for telemetry project...")
        telemetry_config_name = gcloud_config_service.get_config_name_for_project(project_id, "telemetry")
        telemetry_config = await gcloud_config_service.create_configuration(
            config_name=telemetry_config_name,
            project_id=project_id,
            account_email=account
        )
        logger.info(f"✓ Created telemetry configuration: {telemetry_config_name}")

        # Step 4: Check Compute API (BLOCKING)
        logger.info("Bootstrap Step 4: Checking Compute Engine API...")
        compute_check = await check_compute_api_enabled(project_id)

        # Step 5: Enable Required APIs (AUTO-ENABLE)
        logger.info("Bootstrap Step 5: Enabling required APIs...")
        apis = await api_service.enable_apis(project_id)

        # Step 6: Check VPC Networks (BLOCKING)
        logger.info("Bootstrap Step 6: Checking VPC networks...")
        networks = await network_service.list_networks(project_id)

        # Check if no networks exist at all
        if len(networks) == 0:
            raise Exception(
                f"No VPC networks found in project '{project_id}'.\n\n"
                f"You must create at least one custom VPC network before deployment.\n\n"
                f"Required Steps:\n"
                f"1. Go to VPC Networks in GCP Console\n"
                f"2. Create a new VPC network (e.g., 'production-vpc')\n"
                f"3. Add subnets in your target region(s)\n"
                f"4. Configure firewall rules\n"
                f"5. Return here to run Bootstrap again\n\n"
                f"Documentation: https://cloud.google.com/vpc/docs/create-modify-vpc-networks"
            )

        # Check if only default network exists (no custom networks)
        custom_networks = [n for n in networks if n["name"] != "default"]
        if len(custom_networks) == 0:
            raise Exception(
                f"Only default VPC network found in project '{project_id}'.\n\n"
                f"For production deployments, you must create a custom VPC network "
                f"with proper subnet configuration.\n\n"
                f"Required Steps:\n"
                f"1. Go to VPC Networks in GCP Console\n"
                f"2. Create a new VPC network (e.g., 'production-vpc')\n"
                f"3. Add subnets in your target region(s)\n"
                f"4. Configure firewall rules\n"
                f"5. Return here to run Bootstrap again\n\n"
                f"Why this matters: Default networks lack proper isolation, security "
                f"controls, and IP range customization needed for production workloads.\n\n"
                f"Documentation: https://cloud.google.com/vpc/docs/create-modify-vpc-networks"
            )

        logger.info("Bootstrap completed successfully")
        return ApiResponse(
            success=True,
            data={
                "dependencies": deps,
                "account": account,
                "telemetry_config_name": telemetry_config_name,
                "telemetry_config_status": telemetry_config.get("status"),
                "compute_api_enabled": True,
                "apis_enabled": apis,
                "networks_ok": True,
                "network_count": len(networks)
            },
            message="Bootstrap completed successfully"
        )

    except Exception as e:
        logger.error(f"Bootstrap failed: {str(e)}")
        return ApiResponse(success=False, error=str(e))


# Step 1: Verify dependencies
@app.post("/api/verify-dependencies", response_model=ApiResponse)
async def verify_dependencies():
    """
    Verify that required dependencies are installed:
    - gcloud CLI
    - gemini CLI
    - Python (obviously present if we're running)
    - Billing enabled
    """
    logger.info("Verifying dependencies...")

    try:
        from services import dependency_service
        result = await dependency_service.verify_dependencies()

        return ApiResponse(
            success=True,
            data={"dependencies": result}
        )
    except Exception as e:
        logger.error(f"Dependency verification failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 2: Validate configuration
@app.post("/api/validate-config", response_model=ApiResponse)
async def validate_config(config: DeploymentConfig):
    """Validate the deployment configuration."""
    logger.info(f"Validating config for Gemini CLI project: {config.geminiCliProjectId}, telemetry project: {config.telemetryProjectId}")

    try:
        from services import config_service
        await config_service.validate_config(config)

        return ApiResponse(
            success=True,
            message="Configuration is valid"
        )
    except Exception as e:
        logger.error(f"Config validation failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 3: Authenticate
@app.post("/api/authenticate", response_model=ApiResponse)
async def authenticate():
    """Authenticate with Google Cloud."""
    logger.info("Authenticating with Google Cloud...")

    try:
        from services import auth_service
        result = await auth_service.authenticate()

        return ApiResponse(
            success=True,
            data=result,
            message="Authentication successful"
        )
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Gemini CLI OAuth Authentication (NEW)
@app.post("/api/authenticate-gemini-oauth", response_model=ApiResponse)
async def authenticate_gemini_oauth(request: dict):
    """
    Authenticate with Gemini CLI project using Google Login OAuth flow.

    This is used when the Gemini CLI project is different from the
    telemetry project and needs separate authentication.

    Opens a browser window for user to authenticate. Provides manual
    URL as fallback if automatic browser open fails.

    Args:
        request: Dict containing projectId

    Returns:
        ApiResponse with auth status or error
    """
    project_id = request.get("projectId")
    logger.info(f"Starting OAuth authentication for Gemini CLI project: {project_id}")

    try:
        from services import auth_service
        result = await auth_service.authenticate_oauth_flow(project_id)

        return ApiResponse(
            success=True,
            data=result,
            message="OAuth authentication successful"
        )
    except Exception as e:
        logger.error(f"OAuth authentication failed: {str(e)}")
        return ApiResponse(success=False, error=str(e))


# Step 4: Check permissions
@app.post("/api/check-permissions", response_model=ApiResponse)
async def check_permissions(request: dict):
    """Check IAM permissions for the project."""
    project_id = request.get("projectId")
    logger.info(f"Checking permissions for project: {project_id}")

    try:
        from services import iam_service
        result = await iam_service.check_permissions(project_id)

        return ApiResponse(
            success=True,
            data=result
        )
    except Exception as e:
        logger.error(f"Permission check failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 5: Enable APIs
@app.post("/api/enable-apis", response_model=ApiResponse)
async def enable_apis(request: dict):
    """Enable required GCP APIs."""
    project_id = request.get("projectId")
    logger.info(f"Enabling APIs for project: {project_id}")

    try:
        from services import api_service
        result = await api_service.enable_apis(project_id)

        return ApiResponse(
            success=True,
            data=result
        )
    except Exception as e:
        logger.error(f"API enablement failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 6: Configure telemetry
@app.post("/api/configure-telemetry", response_model=ApiResponse)
async def configure_telemetry(request: dict):
    """Configure Gemini CLI telemetry settings and environment variables."""
    log_prompts = request.get("logPrompts", False)
    gemini_cli_project_id = request.get("geminiCliProjectId")
    telemetry_project_id = request.get("telemetryProjectId")
    auth_method = request.get("geminiAuthMethod", "oauth")
    gemini_region = request.get("geminiRegion")

    # Validation: Vertex AI requires region
    if auth_method == "vertex-ai" and not gemini_region:
        logger.error("Gemini region is required for Vertex AI authentication")
        return ApiResponse(
            success=False,
            error="Gemini region is required for Vertex AI authentication"
        )

    logger.info(f"Configuring telemetry (log_prompts={log_prompts}, gemini_cli={gemini_cli_project_id}, telemetry={telemetry_project_id}, auth={auth_method}, region={gemini_region})...")

    try:
        from services import telemetry_service
        result = await telemetry_service.configure_telemetry(
            log_prompts,
            gemini_cli_project_id,
            telemetry_project_id,
            auth_method,
            gemini_region
        )

        return ApiResponse(
            success=True,
            data=result,
            message=f"Telemetry configured successfully with {auth_method} authentication"
        )
    except Exception as e:
        logger.error(f"Telemetry configuration failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 7: Create dataset
@app.post("/api/create-dataset", response_model=ApiResponse)
async def create_dataset(request: dict):
    """Create BigQuery dataset (optionally skip table creation for experiment)."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    region = request.get("region")
    skip_table_creation = request.get("skipTableCreation", False)

    if skip_table_creation:
        logger.info(f"EXPERIMENT MODE: Creating dataset {dataset_name} WITHOUT table (sink will auto-create)...")
    else:
        logger.info(f"Creating dataset {dataset_name} in {region}...")

    try:
        from services import bigquery_service
        result = await bigquery_service.create_dataset(project_id, dataset_name, region, skip_table_creation)

        return ApiResponse(
            success=True,
            data=result,
            message=f"Dataset {dataset_name} created successfully"
        )
    except Exception as e:
        logger.error(f"Dataset creation failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 7b: Create analytics view
@app.post("/api/create-analytics-view", response_model=ApiResponse)
async def create_analytics_view(request: dict):
    """Create BigQuery analytics view for easy querying of telemetry data."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    pseudoanonymize_pii = request.get("pseudoanonymizePii", False)
    logger.info(f"Creating analytics view in dataset {dataset_name} (pseudoanonymize: {pseudoanonymize_pii})...")

    try:
        from services import bigquery_service
        result = await bigquery_service.create_analytics_view(
            project_id,
            dataset_name,
            pseudoanonymize_pii
        )

        return ApiResponse(
            success=True,
            data=result,
            message=f"Analytics view created successfully ({'pseudoanonymized' if pseudoanonymize_pii else 'standard'})"
        )
    except Exception as e:
        logger.error(f"Analytics view creation failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 8: Create Pub/Sub resources
@app.post("/api/create-pubsub", response_model=ApiResponse)
async def create_pubsub(request: dict):
    """Create Pub/Sub topic and subscription for ELT pipeline."""
    project_id = request.get("projectId")
    sink_service_account = request.get("sinkServiceAccount")
    logger.info(f"Creating Pub/Sub resources for project: {project_id}...")

    try:
        from services import pubsub_service
        result = await pubsub_service.create_pubsub_resources(
            project_id=project_id,
            sink_service_account=sink_service_account
        )

        return ApiResponse(
            success=True,
            data=result,
            message="Pub/Sub resources created successfully"
        )
    except Exception as e:
        logger.error(f"Pub/Sub creation failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 8b: Set up GCS bucket and upload UDF
@app.post("/api/setup-gcs", response_model=ApiResponse)
async def setup_gcs(request: dict):
    """Set up GCS bucket and upload UDF for Dataflow pipeline."""
    project_id = request.get("projectId")
    region = request.get("region", "us-central1")
    logger.info(f"Setting up GCS for Dataflow in project: {project_id}...")

    try:
        from services import gcs_service
        result = await gcs_service.setup_gcs_for_dataflow(
            project_id=project_id,
            region=region
        )

        return ApiResponse(
            success=True,
            data=result,
            message="GCS bucket and UDF set up successfully for Dataflow"
        )
    except Exception as e:
        logger.error(f"GCS setup failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 8c: Start Dataflow pipeline
@app.post("/api/start-dataflow", response_model=ApiResponse)
async def start_dataflow(request: dict):
    """Start Dataflow streaming pipeline for ELT processing."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    region = request.get("region", "us-central1")
    network = request.get("network")
    subnetwork = request.get("subnetwork")
    logger.info(f"Starting Dataflow pipeline for project: {project_id}...")

    try:
        from services import dataflow_service
        result = await dataflow_service.start_dataflow_job(
            project_id=project_id,
            dataset_name=dataset_name,
            region=region,
            network=network,
            subnetwork=subnetwork
        )

        return ApiResponse(
            success=True,
            data=result,
            message="Dataflow pipeline started successfully"
        )
    except Exception as e:
        logger.error(f"Dataflow start failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 8c: Get Dataflow job status
@app.post("/api/dataflow-status", response_model=ApiResponse)
async def get_dataflow_status(request: dict):
    """Get status of a Dataflow job."""
    project_id = request.get("projectId")
    job_id = request.get("jobId")
    region = request.get("region", "us-central1")
    logger.info(f"Getting Dataflow job status for job {job_id}...")

    try:
        from services import dataflow_service
        result = await dataflow_service.get_job_status(
            project_id=project_id,
            job_id=job_id,
            region=region
        )

        return ApiResponse(
            success=True,
            data=result
        )
    except Exception as e:
        logger.error(f"Failed to get job status: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 9: Test logging
@app.post("/api/test-logging", response_model=ApiResponse)
async def test_logging(request: dict):
    """Test Gemini CLI logging by running a gemini command and checking Cloud Logging.

    In two-project setup, projectId is the Gemini CLI project where logs are generated.
    """
    project_id = request.get("projectId")
    logger.info(f"Testing Gemini CLI logging for project: {project_id}")

    try:
        from services import logging_service
        # project_id is always the Gemini CLI project (where logs should appear)
        # Pass it as gemini_cli_project_id to use the correct configuration if available
        result = await logging_service.test_gemini_cli_logging(
            project_id=project_id,
            gemini_cli_project_id=project_id  # Use same as project_id
        )

        return ApiResponse(
            success=True,
            data=result,
            message="Test log entry sent successfully"
        )
    except Exception as e:
        logger.error(f"Logging test failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 10: Create sink to Pub/Sub
@app.post("/api/create-sink", response_model=ApiResponse)
async def create_sink(request: dict):
    """Create Cloud Logging sink to Pub/Sub topic (ELT pattern).

    In two-project setup:
    - Sink is created in TELEMETRY project
    - Sink filters logs from GEMINI CLI project (cross-project routing)
    - Destination is Pub/Sub topic in TELEMETRY project
    """
    gemini_cli_project_id = request.get("geminiCliProjectId")
    telemetry_project_id = request.get("telemetryProjectId")
    topic_name = request.get("topicName", "gemini-telemetry-topic")

    logger.info(f"Creating log sink to Pub/Sub topic {topic_name}...")
    logger.info(f"  Gemini CLI project (log source): {gemini_cli_project_id}")
    logger.info(f"  Telemetry project (sink + topic): {telemetry_project_id}")

    try:
        from services import sink_service
        result = await sink_service.create_sink(
            gemini_cli_project_id=gemini_cli_project_id,
            telemetry_project_id=telemetry_project_id,
            topic_name=topic_name
        )

        return ApiResponse(
            success=True,
            data=result,
            message="Log sink created successfully in telemetry project"
        )
    except Exception as e:
        logger.error(f"Sink creation failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 10: Verify sink
@app.post("/api/verify-sink", response_model=ApiResponse)
async def verify_sink(request: dict):
    """Verify that the log sink is working."""
    project_id = request.get("projectId")
    sink_name = request.get("sinkName")
    logger.info(f"Verifying sink {sink_name}...")

    try:
        from services import sink_service
        result = await sink_service.verify_sink(project_id, sink_name)

        return ApiResponse(
            success=True,
            data=result,
            message="Sink verified successfully"
        )
    except Exception as e:
        logger.error(f"Sink verification failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 11: Verify Dataflow pipeline
@app.post("/api/verify-dataflow", response_model=ApiResponse)
async def verify_dataflow(request: dict):
    """Verify Dataflow streaming job is running."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    region = request.get("region", "us-central1")
    logger.info(f"Verifying Dataflow pipeline...")

    try:
        from services import dataflow_service
        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name, region)

        return ApiResponse(
            success=result.get("is_running", False),
            data=result,
            message="Dataflow pipeline verification completed"
        )
    except Exception as e:
        logger.error(f"Dataflow verification failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 11: Verify ELT pipeline
@app.post("/api/verify-elt-pipeline", response_model=ApiResponse)
async def verify_elt_pipeline(request: dict):
    """Verify complete ELT pipeline (Sink → Pub/Sub → Dataflow → BigQuery)."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    region = request.get("region", "us-central1")
    logger.info(f"Verifying complete ELT pipeline...")

    try:
        from services import verification_service
        result = await verification_service.verify_elt_pipeline(project_id, dataset_name, region)

        return ApiResponse(
            success=result.get("pipeline_ready", False),
            data=result,
            message="ELT pipeline verification completed"
        )
    except Exception as e:
        logger.error(f"ELT pipeline verification failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 12: End-to-end verification
@app.post("/api/verify-end-to-end", response_model=ApiResponse)
async def verify_end_to_end(request: dict):
    """Perform end-to-end verification of the ELT pipeline."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    region = request.get("region", "us-central1")
    logger.info(f"Performing ELT pipeline end-to-end verification...")

    try:
        from services import verification_service
        result = await verification_service.verify_end_to_end(
            project_id, dataset_name, region
        )

        return ApiResponse(
            success=result.get("success", False),
            data=result,
            message="End-to-end verification completed"
        )
    except Exception as e:
        logger.error(f"E2E verification failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 13: Create all analytics views
@app.post("/api/create-analytics-views", response_model=ApiResponse)
async def create_analytics_views(request: dict):
    """Create all analytics views (materialized views, regular views, scheduled queries)."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")
    use_pseudonyms = request.get("pseudoanonymizePii", False)

    logger.info(f"Creating analytics views in dataset {dataset_name} (pseudonyms: {use_pseudonyms})...")

    try:
        from services import bigquery_views_service
        result = await bigquery_views_service.create_all_analytics_views(
            project_id,
            dataset_name,
            use_pseudonyms
        )

        created_count = len(result.get("created", []))
        failed_count = len(result.get("failed", []))

        return ApiResponse(
            success=True,
            data=result,
            message=f"Analytics views created: {created_count} succeeded, {failed_count} failed"
        )
    except Exception as e:
        logger.error(f"Analytics views creation failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Step 14: Verify analytics views
@app.post("/api/verify-analytics-views", response_model=ApiResponse)
async def verify_analytics_views(request: dict):
    """Verify all analytics views were created successfully."""
    project_id = request.get("projectId")
    dataset_name = request.get("datasetName")

    logger.info(f"Verifying analytics views in dataset {dataset_name}...")

    try:
        from services import bigquery_views_service
        result = await bigquery_views_service.verify_all_analytics_views(
            project_id,
            dataset_name
        )

        verified_count = result.get("verified_count", 0)
        missing_count = len(result.get("missing_views", [])) + len(result.get("missing_tables", []))

        if missing_count > 0:
            return ApiResponse(
                success=False,
                data=result,
                message=f"Verified {verified_count}/16 views. {missing_count} missing."
            )
        else:
            return ApiResponse(
                success=True,
                data=result,
                message=f"All 16 analytics views verified successfully"
            )
    except Exception as e:
        logger.error(f"Analytics views verification failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Get networks and subnets for a project and region
@app.post("/api/get-networks-and-subnets", response_model=ApiResponse)
async def get_networks_and_subnets(request: dict):
    """Get available VPC networks and subnets for a project and region."""
    project_id = request.get("projectId")
    region = request.get("region")

    logger.info(f"Fetching networks and subnets for project {project_id}, region {region}")

    try:
        from services import network_service
        result = await network_service.get_networks_and_subnets(project_id, region)

        return ApiResponse(
            success=True,
            data=result
        )
    except Exception as e:
        logger.error(f"Failed to fetch networks and subnets: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )




# Cleanup: Delete gcloud configuration
@app.post("/api/cleanup-configuration", response_model=ApiResponse)
async def cleanup_configuration(request: dict):
    """Delete a gcloud configuration to clean up after deployment."""
    config_name = request.get("configName")
    logger.info(f"Deleting gcloud configuration: {config_name}")

    try:
        from services import gcloud_config_service
        result = await gcloud_config_service.delete_configuration(config_name)

        return ApiResponse(
            success=True,
            data=result,
            message=f"Configuration {config_name} deleted successfully"
        )
    except Exception as e:
        logger.error(f"Configuration cleanup failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


# Get deployment status (for StatusPage)
@app.get("/api/status/{deployment_id}", response_model=ApiResponse)
async def get_status(deployment_id: str):
    """Get deployment status by ID."""
    logger.info(f"Getting status for deployment: {deployment_id}")

    try:
        from services import deployment_service
        result = await deployment_service.get_status(deployment_id)

        return ApiResponse(
            success=True,
            data=result
        )
    except Exception as e:
        logger.error(f"Status retrieval failed: {str(e)}")
        return ApiResponse(
            success=False,
            error=str(e)
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
