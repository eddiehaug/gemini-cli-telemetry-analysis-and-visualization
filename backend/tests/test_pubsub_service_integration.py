"""
Integration tests for Pub/Sub service.
Tests complete workflows with mocked GCP services.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import pubsub_service


class TestTopicCreationWorkflow:
    """Integration tests for topic creation workflow"""

    @pytest.mark.asyncio
    async def test_complete_topic_creation_workflow(self):
        """Test complete topic creation workflow"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        # Track workflow steps
        workflow_steps = []

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/{topic_name}")

        def mock_create_topic(request):
            workflow_steps.append("topic_created")
            mock_topic = Mock()
            mock_topic.name = request["name"]
            return mock_topic

        mock_publisher.create_topic = mock_create_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.create_topic(project_id, topic_name)

            # Verify workflow completed
            assert "topic_created" in workflow_steps
            assert result["status"] == "created"
            assert result["name"] == topic_name

    @pytest.mark.asyncio
    async def test_topic_path_construction(self):
        """Test that topic path is constructed correctly"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/{topic_name}")

        mock_topic = Mock()
        mock_topic.name = f"projects/{project_id}/topics/{topic_name}"
        mock_publisher.create_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.create_topic(project_id, topic_name)

            # Verify topic path format
            assert f"projects/{project_id}/topics/{topic_name}" == result["topic"]


class TestSubscriptionCreationWorkflow:
    """Integration tests for subscription creation workflow"""

    @pytest.mark.asyncio
    async def test_complete_subscription_creation_workflow(self):
        """Test complete subscription creation workflow"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        subscription_name = "gemini-telemetry-sub"

        # Track workflow steps
        workflow_steps = []

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/{topic_name}")

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/{subscription_name}")

        def mock_create_subscription(request):
            workflow_steps.append("subscription_created")
            # Verify request structure
            assert "name" in request
            assert "topic" in request
            mock_subscription = Mock()
            mock_subscription.name = request["name"]
            return mock_subscription

        mock_subscriber.create_subscription = mock_create_subscription

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                result = await pubsub_service.create_subscription(project_id, topic_name, subscription_name)

                # Verify workflow completed
                assert "subscription_created" in workflow_steps
                assert result["status"] == "created"
                assert result["name"] == subscription_name
                assert result["delivery_type"] == "Pull"

    @pytest.mark.asyncio
    async def test_subscription_references_topic(self):
        """Test that subscription correctly references topic"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        subscription_name = "gemini-telemetry-sub"

        mock_publisher = Mock()
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        mock_publisher.topic_path = Mock(return_value=topic_path)

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/{subscription_name}")

        # Capture the request to verify topic reference
        request_captured = None

        def mock_create_subscription(request):
            nonlocal request_captured
            request_captured = request
            mock_subscription = Mock()
            mock_subscription.name = request["name"]
            return mock_subscription

        mock_subscriber.create_subscription = mock_create_subscription

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                result = await pubsub_service.create_subscription(project_id, topic_name, subscription_name)

                # Verify topic reference in request
                assert request_captured is not None
                assert request_captured["topic"] == topic_path


class TestIAMPermissionGrantWorkflow:
    """Integration tests for IAM permission grant workflow"""

    @pytest.mark.asyncio
    async def test_complete_iam_grant_workflow(self, monkeypatch):
        """Test complete IAM permission grant workflow"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"
        topic_name = "gemini-telemetry-topic"

        # Track workflow steps
        workflow_steps = []

        mock_publisher = Mock()
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        mock_publisher.topic_path = Mock(return_value=topic_path)

        # Mock IAM policy operations
        def mock_get_iam_policy(request):
            workflow_steps.append("get_policy")
            mock_policy = Mock()
            mock_policy.bindings = []
            return mock_policy

        def mock_set_iam_policy(request):
            workflow_steps.append("set_policy")
            # Verify policy has new binding
            policy = request["policy"]
            assert len(policy.bindings) > 0
            return policy

        mock_publisher.get_iam_policy = mock_get_iam_policy
        mock_publisher.set_iam_policy = mock_set_iam_policy

        # Mock sleep
        async def mock_sleep(seconds):
            workflow_steps.append(f"wait_{seconds}s")

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account,
                topic_name=topic_name
            )

            # Verify complete workflow
            assert "get_policy" in workflow_steps
            assert "set_policy" in workflow_steps
            assert "wait_90s" in workflow_steps

            # Verify result
            assert result["status"] == "granted"
            assert result["service_account"] == sink_service_account
            assert result["role"] == "roles/pubsub.publisher"

    @pytest.mark.asyncio
    async def test_iam_policy_binding_structure(self, monkeypatch):
        """Test that IAM policy binding has correct structure"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        # Capture the policy that gets set
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
            await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )

            # Verify policy structure
            assert policy_captured is not None
            assert len(policy_captured.bindings) > 0
            # Verify binding has correct role and member
            binding = policy_captured.bindings[0]
            assert binding.role == "roles/pubsub.publisher"
            assert f"serviceAccount:{sink_service_account}" in binding.members


class TestVerificationWorkflows:
    """Integration tests for verification workflows"""

    @pytest.mark.asyncio
    async def test_verify_topic_workflow(self):
        """Test topic verification workflow"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        mock_publisher = Mock()
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        mock_publisher.topic_path = Mock(return_value=topic_path)

        # Mock get_topic
        mock_topic = Mock()
        mock_publisher.get_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            result = await pubsub_service.verify_topic_exists(project_id, topic_name)

            # Verify get_topic was called with correct request
            assert mock_publisher.get_topic.called
            # call_args[1] contains kwargs, which has 'request' dict
            call_kwargs = mock_publisher.get_topic.call_args[1]
            assert "request" in call_kwargs
            assert call_kwargs["request"]["topic"] == topic_path

            assert result is True

    @pytest.mark.asyncio
    async def test_verify_subscription_workflow(self):
        """Test subscription verification workflow"""
        project_id = "test-project-123"
        subscription_name = "gemini-telemetry-sub"

        mock_subscriber = Mock()
        subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"
        mock_subscriber.subscription_path = Mock(return_value=subscription_path)

        # Mock get_subscription
        mock_subscription = Mock()
        mock_subscriber.get_subscription.return_value = mock_subscription

        with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
            result = await pubsub_service.verify_subscription_exists(project_id, subscription_name)

            # Verify get_subscription was called
            assert mock_subscriber.get_subscription.called
            assert result is True


class TestErrorHandling:
    """Integration tests for error handling"""

    @pytest.mark.asyncio
    async def test_topic_creation_failure_raises_exception(self):
        """Test that topic creation failure raises exception"""
        project_id = "test-project-123"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")
        mock_publisher.create_topic.side_effect = Exception("API Error")

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with pytest.raises(Exception) as exc_info:
                await pubsub_service.create_topic(project_id)

            assert "API Error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_subscription_creation_failure_raises_exception(self):
        """Test that subscription creation failure raises exception"""
        project_id = "test-project-123"

        mock_publisher = Mock()
        mock_publisher.topic_path = Mock(return_value=f"projects/{project_id}/topics/gemini-telemetry-topic")

        mock_subscriber = Mock()
        mock_subscriber.subscription_path = Mock(return_value=f"projects/{project_id}/subscriptions/gemini-telemetry-sub")
        mock_subscriber.create_subscription.side_effect = Exception("Subscription error")

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
                with pytest.raises(Exception) as exc_info:
                    await pubsub_service.create_subscription(project_id)

                assert "Subscription error" in str(exc_info.value)


class TestFullResourceCreation:
    """Integration tests for create_pubsub_resources convenience function"""

    @pytest.mark.asyncio
    async def test_creates_all_resources_in_order(self, monkeypatch):
        """Test that all resources are created in correct order"""
        project_id = "test-project-123"

        # Track execution order
        execution_order = []

        async def mock_create_topic(pid, topic_name="gemini-telemetry-topic"):
            execution_order.append("topic")
            return {
                "topic": f"projects/{pid}/topics/{topic_name}",
                "status": "created"
            }

        async def mock_create_subscription(pid, topic_name="gemini-telemetry-topic", subscription_name="gemini-telemetry-sub"):
            execution_order.append("subscription")
            return {
                "subscription": f"projects/{pid}/subscriptions/{subscription_name}",
                "status": "created"
            }

        monkeypatch.setattr(pubsub_service, 'create_topic', mock_create_topic)
        monkeypatch.setattr(pubsub_service, 'create_subscription', mock_create_subscription)

        result = await pubsub_service.create_pubsub_resources(project_id)

        # Verify execution order: topic must be created before subscription
        assert execution_order == ["topic", "subscription"]

        # Verify result structure
        assert "topic" in result
        assert "subscription" in result

    @pytest.mark.asyncio
    async def test_includes_permissions_when_sink_provided(self, monkeypatch):
        """Test that permissions are granted when sink service account is provided"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        execution_order = []

        async def mock_create_topic(pid, topic_name="gemini-telemetry-topic"):
            execution_order.append("topic")
            return {"topic": f"projects/{pid}/topics/{topic_name}", "status": "created"}

        async def mock_create_subscription(pid, topic_name="gemini-telemetry-topic", subscription_name="gemini-telemetry-sub"):
            execution_order.append("subscription")
            return {"subscription": f"projects/{pid}/subscriptions/{subscription_name}", "status": "created"}

        async def mock_grant_publisher(project_id, sink_service_account, topic_name="gemini-telemetry-topic"):
            execution_order.append("permissions")
            return {"status": "granted", "service_account": sink_service_account}

        monkeypatch.setattr(pubsub_service, 'create_topic', mock_create_topic)
        monkeypatch.setattr(pubsub_service, 'create_subscription', mock_create_subscription)
        monkeypatch.setattr(pubsub_service, 'grant_publisher_to_sink', mock_grant_publisher)

        result = await pubsub_service.create_pubsub_resources(
            project_id=project_id,
            sink_service_account=sink_service_account
        )

        # Verify all three operations executed
        assert execution_order == ["topic", "subscription", "permissions"]

        # Verify result includes permissions
        assert "permissions" in result
        assert result["permissions"]["service_account"] == sink_service_account
