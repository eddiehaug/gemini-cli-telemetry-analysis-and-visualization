"""
Pub/Sub service for Gemini CLI telemetry deployment.

This module handles creation of Pub/Sub topics and subscriptions for the ELT pipeline:
- Cloud Logging → Pub/Sub Topic → Dataflow → BigQuery

Per IMPLEMENTATION_PLAN.md Step 8:
- Create topic: gemini-telemetry-topic
- Create subscription: gemini-telemetry-sub (Pull delivery type)
- Grant Pub/Sub Publisher role to sink service account
"""

import logging
import asyncio
from typing import Dict
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import resourcemanager_v3
from utils.validators import (
    validate_gcp_project_id,
    validate_topic_name,
    ValidationError
)

logger = logging.getLogger(__name__)


async def create_topic(project_id: str, topic_name: str = "gemini-telemetry-topic") -> Dict:
    """
    Create a Pub/Sub topic for receiving Cloud Logging sink data.

    Args:
        project_id: GCP project ID
        topic_name: Name of the topic to create (default: gemini-telemetry-topic)

    Returns:
        Dict with topic details:
        {
            "topic": "projects/{project}/topics/{topic_name}",
            "name": topic_name,
            "status": "created" or "already_exists"
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        topic_name = validate_topic_name(topic_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)

        try:
            # Attempt to create the topic
            topic = publisher.create_topic(request={"name": topic_path})
            logger.info(f"✓ Pub/Sub topic created: {topic.name}")

            return {
                "topic": topic.name,
                "name": topic_name,
                "status": "created",
                "note": f"Topic {topic_name} created for Cloud Logging sink"
            }

        except AlreadyExists:
            logger.info(f"Topic {topic_path} already exists")

            return {
                "topic": topic_path,
                "name": topic_name,
                "status": "already_exists",
                "note": f"Topic {topic_name} already exists and will be reused"
            }

    except Exception as e:
        logger.error(f"Failed to create Pub/Sub topic: {str(e)}")
        raise


async def create_subscription(
    project_id: str,
    topic_name: str = "gemini-telemetry-topic",
    subscription_name: str = "gemini-telemetry-sub"
) -> Dict:
    """
    Create a Pull subscription for Dataflow to consume messages from the topic.

    If subscription already exists, delete and recreate it to ensure it points
    to the current topic (avoids "_deleted-topic_" errors).

    Args:
        project_id: GCP project ID
        topic_name: Name of the topic to subscribe to
        subscription_name: Name of the subscription to create

    Returns:
        Dict with subscription details:
        {
            "subscription": "projects/{project}/subscriptions/{subscription_name}",
            "name": subscription_name,
            "topic": topic_path,
            "delivery_type": "Pull",
            "status": "created" or "already_exists"
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        topic_name = validate_topic_name(topic_name)
        subscription_name = validate_topic_name(subscription_name)  # Same validation rules
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()

        topic_path = publisher.topic_path(project_id, topic_name)
        subscription_path = subscriber.subscription_path(project_id, subscription_name)

        try:
            # Create Pull subscription
            subscription = subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path
                }
            )

            logger.info(f"✓ Pull subscription created: {subscription.name}")
            logger.info(f"  - Topic: {topic_path}")
            logger.info(f"  - Delivery type: Pull (for Dataflow)")

            return {
                "subscription": subscription.name,
                "name": subscription_name,
                "topic": topic_path,
                "delivery_type": "Pull",
                "status": "created",
                "note": f"Subscription {subscription_name} created for Dataflow consumption"
            }

        except AlreadyExists:
            logger.info(f"Subscription {subscription_path} already exists - deleting and recreating to ensure correct topic binding...")

            # Delete existing subscription to avoid stale topic references
            try:
                subscriber.delete_subscription(request={"subscription": subscription_path})
                logger.info(f"  Deleted old subscription")
            except NotFound:
                logger.info(f"  Subscription already deleted")

            # Recreate subscription with current topic
            subscription = subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path
                }
            )

            logger.info(f"✓ Pull subscription recreated: {subscription.name}")
            logger.info(f"  - Topic: {topic_path}")
            logger.info(f"  - Delivery type: Pull (for Dataflow)")

            return {
                "subscription": subscription.name,
                "name": subscription_name,
                "topic": topic_path,
                "delivery_type": "Pull",
                "status": "recreated",
                "note": f"Subscription {subscription_name} recreated to bind to current topic"
            }

    except Exception as e:
        logger.error(f"Failed to create Pub/Sub subscription: {str(e)}")
        raise


async def grant_publisher_to_sink(
    project_id: str,
    sink_service_account: str,
    topic_name: str = "gemini-telemetry-topic"
) -> Dict:
    """
    Grant Pub/Sub Publisher role to the Cloud Logging sink's service account.

    This allows the sink to publish log entries to the Pub/Sub topic.

    Args:
        project_id: GCP project ID
        sink_service_account: Service account email of the Cloud Logging sink
        topic_name: Name of the topic to grant permissions on

    Returns:
        Dict with permission grant details:
        {
            "topic": topic_path,
            "service_account": sink_service_account,
            "role": "roles/pubsub.publisher",
            "status": "granted",
            "iam_propagation_wait": 90
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        topic_name = validate_topic_name(topic_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)

        # Get current IAM policy
        policy = publisher.get_iam_policy(request={"resource": topic_path})

        # Add the binding if not already present
        binding_exists = False
        for binding in policy.bindings:
            if binding.role == "roles/pubsub.publisher":
                if f"serviceAccount:{sink_service_account}" in binding.members:
                    binding_exists = True
                    logger.info(f"Service account {sink_service_account} already has Publisher role")
                else:
                    # Add to existing binding
                    binding.members.append(f"serviceAccount:{sink_service_account}")
                    binding_exists = True
                break

        if not binding_exists:
            # Create new binding
            from google.cloud.pubsub_v1 import types
            new_binding = types.Binding(
                role="roles/pubsub.publisher",
                members=[f"serviceAccount:{sink_service_account}"]
            )
            policy.bindings.append(new_binding)

        # Set the updated policy
        publisher.set_iam_policy(request={"resource": topic_path, "policy": policy})

        logger.info(f"✓ Granted roles/pubsub.publisher to {sink_service_account}")
        logger.info(f"  - Topic: {topic_path}")
        logger.info(f"  - Waiting 90 seconds for IAM propagation...")

        # Wait for IAM propagation (critical for sink to work)
        await asyncio.sleep(90)

        logger.info(f"✓ IAM propagation complete")

        return {
            "topic": topic_path,
            "service_account": sink_service_account,
            "role": "roles/pubsub.publisher",
            "status": "granted",
            "iam_propagation_wait": 90,
            "note": "Sink can now publish to Pub/Sub topic"
        }

    except Exception as e:
        logger.error(f"Failed to grant Pub/Sub Publisher role: {str(e)}")
        raise


async def verify_topic_exists(project_id: str, topic_name: str = "gemini-telemetry-topic") -> bool:
    """
    Verify that a Pub/Sub topic exists.

    Args:
        project_id: GCP project ID
        topic_name: Name of the topic to verify

    Returns:
        True if topic exists, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        topic_name = validate_topic_name(topic_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)

        publisher.get_topic(request={"topic": topic_path})
        logger.info(f"Topic {topic_path} exists")
        return True

    except NotFound:
        logger.warning(f"Topic {topic_path} not found")
        return False
    except Exception as e:
        logger.error(f"Topic verification failed: {str(e)}")
        return False


async def verify_subscription_exists(
    project_id: str,
    subscription_name: str = "gemini-telemetry-sub"
) -> bool:
    """
    Verify that a Pub/Sub subscription exists.

    Args:
        project_id: GCP project ID
        subscription_name: Name of the subscription to verify

    Returns:
        True if subscription exists, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        subscription_name = validate_topic_name(subscription_name)  # Same validation rules
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)

        subscriber.get_subscription(request={"subscription": subscription_path})
        logger.info(f"Subscription {subscription_path} exists")
        return True

    except NotFound:
        logger.warning(f"Subscription {subscription_path} not found")
        return False
    except Exception as e:
        logger.error(f"Subscription verification failed: {str(e)}")
        return False


async def grant_dataflow_subscription_permissions(
    project_id: str,
    subscription_name: str = "gemini-telemetry-sub",
    topic_name: str = "gemini-telemetry-topic"
) -> Dict:
    """
    Grant Dataflow worker service account permission to consume from subscription.

    The Compute Engine default service account needs:
    - pubsub.editor role on the subscription (includes get, pull, ack, etc.)
    - pubsub.editor role on the topic (to retrieve topic metadata)

    Args:
        project_id: GCP project ID
        subscription_name: Name of the subscription
        topic_name: Name of the topic

    Returns:
        Dict with permission grant status
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        subscription_name = validate_topic_name(subscription_name)  # Same validation rules
        topic_name = validate_topic_name(topic_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Get project number for the Compute Engine default service account
        import subprocess
        result = subprocess.run(
            ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            raise Exception(f"Failed to get project number: {result.stderr}")

        project_number = result.stdout.strip()
        compute_sa = f"serviceAccount:{project_number}-compute@developer.gserviceaccount.com"

        logger.info(f"Granting Pub/Sub permissions to Dataflow worker: {compute_sa}")

        # Grant permissions on SUBSCRIPTION
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)

        # Get existing IAM policy for subscription
        sub_policy = subscriber.get_iam_policy(request={"resource": subscription_path})

        # Grant roles/pubsub.editor instead of subscriber for full access including metadata
        # This includes pubsub.subscriptions.get needed for ack deadline retrieval
        binding_exists = False
        for binding in sub_policy.bindings:
            if binding.role == "roles/pubsub.editor":
                if compute_sa in binding.members:
                    binding_exists = True
                    logger.info(f"Compute Engine SA already has Editor role on subscription")
                else:
                    binding.members.append(compute_sa)
                    binding_exists = True
                break

        if not binding_exists:
            from google.cloud.pubsub_v1 import types
            new_binding = types.Binding(
                role="roles/pubsub.editor",
                members=[compute_sa]
            )
            sub_policy.bindings.append(new_binding)

        # Set the updated policy for subscription
        subscriber.set_iam_policy(request={"resource": subscription_path, "policy": sub_policy})

        logger.info(f"✓ Granted roles/pubsub.editor to Dataflow worker on subscription")
        logger.info(f"  - Subscription: {subscription_path}")

        # Grant permissions on TOPIC (needed to retrieve topic metadata)
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)

        # Get existing IAM policy for topic
        topic_policy = publisher.get_iam_policy(request={"resource": topic_path})

        # Add editor role for topic full access
        editor_binding_exists = False
        for binding in topic_policy.bindings:
            if binding.role == "roles/pubsub.editor":
                if compute_sa in binding.members:
                    editor_binding_exists = True
                    logger.info(f"Compute Engine SA already has Editor role on topic")
                else:
                    binding.members.append(compute_sa)
                    editor_binding_exists = True
                break

        if not editor_binding_exists:
            from google.cloud.pubsub_v1 import types
            new_editor_binding = types.Binding(
                role="roles/pubsub.editor",
                members=[compute_sa]
            )
            topic_policy.bindings.append(new_editor_binding)

        # Set the updated policy for topic
        publisher.set_iam_policy(request={"resource": topic_path, "policy": topic_policy})

        logger.info(f"✓ Granted roles/pubsub.editor to Dataflow worker on topic")
        logger.info(f"  - Topic: {topic_path}")

        # Wait for IAM propagation (critical for Dataflow to access Pub/Sub)
        # Increased to 120 seconds to ensure permissions propagate before Dataflow job starts
        logger.info(f"⏳ Waiting 120 seconds for IAM propagation...")
        await asyncio.sleep(120)
        logger.info(f"✓ IAM propagation complete")

        return {
            "subscription": subscription_path,
            "topic": topic_path,
            "service_account": compute_sa,
            "roles": ["roles/pubsub.editor on subscription", "roles/pubsub.editor on topic"],
            "status": "granted",
            "iam_propagation_wait": 120
        }

    except Exception as e:
        logger.error(f"Failed to grant Dataflow subscription permissions: {str(e)}")
        raise


async def create_pubsub_resources(
    project_id: str,
    sink_service_account: str = None
) -> Dict:
    """
    Create all Pub/Sub resources for the ELT pipeline.

    This is a convenience function that:
    1. Creates the topic
    2. Creates the subscription
    3. Grants Dataflow worker permission to consume from subscription
    4. Grants publisher role to sink service account (if provided)

    Args:
        project_id: GCP project ID
        sink_service_account: Optional sink service account to grant permissions

    Returns:
        Dict with all resource details:
        {
            "topic": {...},
            "subscription": {...},
            "dataflow_permissions": {...},
            "permissions": {...} (if sink_service_account provided)
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        result = {}

        # Create topic
        topic_result = await create_topic(project_id)
        result["topic"] = topic_result

        # Create subscription
        subscription_result = await create_subscription(project_id)
        result["subscription"] = subscription_result

        # Grant Dataflow worker permissions to consume from subscription
        dataflow_perms = await grant_dataflow_subscription_permissions(project_id)
        result["dataflow_permissions"] = dataflow_perms

        # Grant permissions if sink service account provided
        if sink_service_account:
            permissions_result = await grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )
            result["permissions"] = permissions_result

        return result

    except Exception as e:
        logger.error(f"Failed to create Pub/Sub resources: {str(e)}")
        raise
