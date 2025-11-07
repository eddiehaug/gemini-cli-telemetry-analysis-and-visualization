"""
Functional/End-to-End tests for Pub/Sub service.
Tests complete API workflows through FastAPI endpoints.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestPubSubAPIEndToEnd:
    """End-to-end tests for Pub/Sub API endpoints"""

    @pytest.mark.asyncio
    async def test_create_pubsub_api_endpoint(self, test_client, monkeypatch):
        """Test /api/create-pubsub endpoint end-to-end"""
        from services import pubsub_service

        # Mock the create_pubsub_resources function
        async def mock_create_pubsub_resources(project_id, sink_service_account=None):
            return {
                "topic": {
                    "topic": f"projects/{project_id}/topics/gemini-telemetry-topic",
                    "status": "created"
                },
                "subscription": {
                    "subscription": f"projects/{project_id}/subscriptions/gemini-telemetry-sub",
                    "status": "created"
                }
            }

        monkeypatch.setattr(pubsub_service, 'create_pubsub_resources', mock_create_pubsub_resources)

        # Make API request
        response = test_client.post(
            "/api/create-pubsub",
            json={
                "projectId": "test-project-123"
            }
        )

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "topic" in data["data"]
        assert "subscription" in data["data"]


class TestEndToEndWorkflow:
    """End-to-end workflow tests"""

    @pytest.mark.asyncio
    async def test_complete_pubsub_setup_workflow(self, monkeypatch):
        """
        Test complete Pub/Sub setup workflow:
        1. Create topic
        2. Create subscription
        3. Grant publisher permissions
        4. Verify resources exist
        """
        from services import pubsub_service

        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        # Track workflow steps
        workflow_steps = []

        # Setup mocks
        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        def mock_create_topic(request):
            workflow_steps.append("topic_created")
            mock_topic = Mock()
            mock_topic.name = request["name"]
            return mock_topic

        mock_publisher.create_topic = mock_create_topic

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/gemini-telemetry-sub")

        def mock_create_subscription(request):
            workflow_steps.append("subscription_created")
            mock_subscription = Mock()
            mock_subscription.name = request["name"]
            return mock_subscription

        mock_subscriber.create_subscription = mock_create_subscription

        # Mock IAM operations
        mock_policy = Mock()
        mock_policy.bindings = []

        def mock_get_iam_policy(request):
            workflow_steps.append("get_iam_policy")
            return mock_policy

        def mock_set_iam_policy(request):
            workflow_steps.append("set_iam_policy")
            return request["policy"]

        mock_publisher.get_iam_policy = mock_get_iam_policy
        mock_publisher.set_iam_policy = mock_set_iam_policy

        # Mock get_topic/get_subscription for verification
        mock_publisher.get_topic = Mock(return_value=Mock())
        mock_subscriber.get_subscription = Mock(return_value=Mock())

        # Mock sleep
        async def mock_sleep(seconds):
            workflow_steps.append(f"iam_wait_{seconds}s")

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                # Execute complete workflow
                result = await pubsub_service.create_pubsub_resources(
                    project_id=project_id,
                    sink_service_account=sink_service_account
                )

                # Verify workflow completed all steps
                assert "topic_created" in workflow_steps
                assert "subscription_created" in workflow_steps
                assert "get_iam_policy" in workflow_steps
                assert "set_iam_policy" in workflow_steps
                assert "iam_wait_90s" in workflow_steps

                # Verify result structure
                assert "topic" in result
                assert "subscription" in result
                assert "permissions" in result

                # Verify resources exist
                topic_exists = await pubsub_service.verify_topic_exists(project_id)
                subscription_exists = await pubsub_service.verify_subscription_exists(project_id)

                assert topic_exists is True
                assert subscription_exists is True

    @pytest.mark.asyncio
    async def test_workflow_with_existing_resources(self, monkeypatch):
        """
        Test workflow when resources already exist (idempotent behavior).
        """
        from services import pubsub_service

        project_id = "test-project-123"

        # Setup mocks
        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")
        mock_publisher.create_topic.side_effect = AlreadyExists("Topic exists")

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/gemini-telemetry-sub")
        mock_subscriber.create_subscription.side_effect = AlreadyExists("Subscription exists")

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                # Execute workflow
                result = await pubsub_service.create_pubsub_resources(project_id)

                # Should succeed even when resources exist
                assert result["topic"]["status"] == "already_exists"
                assert result["subscription"]["status"] == "already_exists"

    @pytest.mark.asyncio
    async def test_error_recovery_workflow(self):
        """Test error recovery in the workflow"""
        from services import pubsub_service

        project_id = "test-project-123"

        # Simulate topic creation failure
        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")
        mock_publisher.create_topic.side_effect = Exception("Topic creation failed")

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            # Verify exception is raised
            with pytest.raises(Exception) as exc_info:
                await pubsub_service.create_topic(project_id)

            assert "Topic creation failed" in str(exc_info.value)


class TestELTPipelineReadiness:
    """Tests verifying Pub/Sub resources are ready for ELT pipeline"""

    @pytest.mark.asyncio
    async def test_topic_ready_for_sink(self):
        """Test that topic is ready to receive data from Cloud Logging sink"""
        from services import pubsub_service

        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        mock_publisher = Mock()
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        mock_publisher.topic_path = Mock(return_value=topic_path)

        mock_topic = Mock()
        mock_topic.name = topic_path
        mock_publisher.create_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.create_topic(project_id, topic_name)

            # Verify topic path format matches what sink expects
            assert result["topic"] == topic_path
            assert topic_name in result["topic"]

    @pytest.mark.asyncio
    async def test_subscription_ready_for_dataflow(self):
        """Test that subscription is configured for Dataflow consumption"""
        from services import pubsub_service

        project_id = "test-project-123"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/gemini-telemetry-sub")

        mock_subscription = Mock()
        mock_subscription.name = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"
        mock_subscriber.create_subscription.return_value = mock_subscription

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                result = await pubsub_service.create_subscription(project_id)

                # Verify Pull delivery type (required for Dataflow)
                assert result["delivery_type"] == "Pull"
                assert result["status"] in ["created", "already_exists"]

    @pytest.mark.asyncio
    async def test_iam_permissions_enable_sink_publishing(self, monkeypatch):
        """Test that IAM permissions allow sink to publish to topic"""
        from services import pubsub_service

        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        # Mock IAM operations
        policy_captured = None

        def mock_get_iam_policy(request):
            mock_policy = Mock()
            mock_policy.bindings = []
            return mock_policy

        def mock_set_iam_policy(request):
            nonlocal policy_captured
            policy_captured = request["policy"]
            return policy_captured

        mock_publisher.get_iam_policy = mock_get_iam_policy
        mock_publisher.set_iam_policy = mock_set_iam_policy

        async def mock_sleep(seconds):
            pass

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )

            # Verify correct role granted
            assert result["role"] == "roles/pubsub.publisher"

            # Verify policy includes service account
            assert policy_captured is not None
            binding = policy_captured.bindings[0]
            assert f"serviceAccount:{sink_service_account}" in binding.members


class TestResourceNaming:
    """Tests for resource naming conventions"""

    @pytest.mark.asyncio
    async def test_topic_name_matches_specification(self):
        """Test that topic name matches IMPLEMENTATION_PLAN.md specification"""
        from services import pubsub_service

        project_id = "test-project-123"
        expected_topic_name = "gemini-telemetry-topic"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/{expected_topic_name}")

        mock_topic = Mock()
        mock_topic.name = f"projects/{project_id}/topics/{expected_topic_name}"
        mock_publisher.create_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.create_topic(project_id)

            # Verify default topic name
            assert result["name"] == expected_topic_name

    @pytest.mark.asyncio
    async def test_subscription_name_matches_specification(self):
        """Test that subscription name matches IMPLEMENTATION_PLAN.md specification"""
        from services import pubsub_service

        project_id = "test-project-123"
        expected_subscription_name = "gemini-telemetry-sub"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/{expected_subscription_name}")

        mock_subscription = Mock()
        mock_subscription.name = f"projects/{project_id}/subscriptions/{expected_subscription_name}"
        mock_subscriber.create_subscription.return_value = mock_subscription

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                result = await pubsub_service.create_subscription(project_id)

                # Verify default subscription name
                assert result["name"] == expected_subscription_name


class TestIAMPropagation:
    """Tests for IAM propagation wait behavior"""

    @pytest.mark.asyncio
    async def test_iam_propagation_wait_duration(self, monkeypatch):
        """Test that IAM propagation wait is exactly 90 seconds"""
        from services import pubsub_service

        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        mock_policy = Mock()
        mock_policy.bindings = []
        mock_publisher.get_iam_policy.return_value = mock_policy
        mock_publisher.set_iam_policy.return_value = mock_policy

        # Capture sleep duration
        sleep_duration = None

        async def mock_sleep(seconds):
            nonlocal sleep_duration
            sleep_duration = seconds

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )

            # Verify 90-second wait (per IMPLEMENTATION_PLAN.md)
            assert sleep_duration == 90
            assert result["iam_propagation_wait"] == 90

    @pytest.mark.asyncio
    async def test_iam_wait_reported_in_result(self, monkeypatch):
        """Test that IAM propagation wait is reported in result"""
        from services import pubsub_service

        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        mock_policy = Mock()
        mock_policy.bindings = []
        mock_publisher.get_iam_policy.return_value = mock_policy

        async def mock_sleep(seconds):
            pass

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )

            # Verify result includes propagation wait info
            assert "iam_propagation_wait" in result
            assert result["note"] is not None
