"""
Unit tests for Pub/Sub service.
Tests individual functions with mocked dependencies.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import pubsub_service


@pytest.fixture
def mock_publisher_client():
    """Mock Pub/Sub PublisherClient."""
    mock_client = Mock(spec=pubsub_v1.PublisherClient)
    # Make topic_path dynamically return path based on arguments
    mock_client.topic_path = Mock(side_effect=lambda project, topic: f"projects/{project}/topics/{topic}")
    return mock_client


@pytest.fixture
def mock_subscriber_client():
    """Mock Pub/Sub SubscriberClient."""
    mock_client = Mock(spec=pubsub_v1.SubscriberClient)
    mock_client.subscription_path = Mock(return_value="projects/test-project/subscriptions/gemini-telemetry-sub")
    return mock_client


class TestCreateTopic:
    """Test create_topic function"""

    @pytest.mark.asyncio
    async def test_create_topic_success(self, mock_publisher_client):
        """Test successful topic creation"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        # Mock topic creation
        mock_topic = Mock()
        mock_topic.name = f"projects/{project_id}/topics/{topic_name}"
        mock_publisher_client.create_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.create_topic(project_id, topic_name)

            # Verify create_topic was called
            assert mock_publisher_client.create_topic.called

            # Verify result
            assert result["topic"] == mock_topic.name
            assert result["name"] == topic_name
            assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_topic_already_exists(self, mock_publisher_client):
        """Test topic creation when topic already exists"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        # Mock AlreadyExists exception
        mock_publisher_client.create_topic.side_effect = AlreadyExists("Topic exists")
        topic_path = f"projects/{project_id}/topics/{topic_name}"

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.create_topic(project_id, topic_name)

            # Verify result indicates already exists
            assert result["status"] == "already_exists"
            assert result["name"] == topic_name
            assert topic_path in result["topic"]

    @pytest.mark.asyncio
    async def test_create_topic_with_custom_name(self, mock_publisher_client):
        """Test topic creation with custom topic name"""
        project_id = "test-project-123"
        custom_topic = "custom-topic"

        mock_topic = Mock()
        mock_topic.name = f"projects/{project_id}/topics/{custom_topic}"
        mock_publisher_client.create_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.create_topic(project_id, custom_topic)

            assert result["name"] == custom_topic
            assert custom_topic in result["topic"]


class TestCreateSubscription:
    """Test create_subscription function"""

    @pytest.mark.asyncio
    async def test_create_subscription_success(self, mock_publisher_client, mock_subscriber_client):
        """Test successful subscription creation"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        subscription_name = "gemini-telemetry-sub"

        # Mock subscription creation
        mock_subscription = Mock()
        mock_subscription.name = f"projects/{project_id}/subscriptions/{subscription_name}"
        mock_subscriber_client.create_subscription.return_value = mock_subscription

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber_client):
                result = await pubsub_service.create_subscription(project_id, topic_name, subscription_name)

                # Verify create_subscription was called
                assert mock_subscriber_client.create_subscription.called

                # Verify result
                assert result["subscription"] == mock_subscription.name
                assert result["name"] == subscription_name
                assert result["delivery_type"] == "Pull"
                assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_subscription_already_exists(self, mock_publisher_client, mock_subscriber_client):
        """Test subscription creation when subscription already exists"""
        project_id = "test-project-123"
        subscription_name = "gemini-telemetry-sub"

        # Mock AlreadyExists exception
        mock_subscriber_client.create_subscription.side_effect = AlreadyExists("Subscription exists")

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber_client):
                result = await pubsub_service.create_subscription(project_id)

                # Verify result indicates already exists
                assert result["status"] == "already_exists"
                assert result["name"] == subscription_name

    @pytest.mark.asyncio
    async def test_subscription_references_correct_topic(self, mock_publisher_client, mock_subscriber_client):
        """Test that subscription references the correct topic"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        mock_subscription = Mock()
        mock_subscription.name = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"
        mock_subscriber_client.create_subscription.return_value = mock_subscription

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber_client):
                result = await pubsub_service.create_subscription(project_id, topic_name)

                # Verify topic reference
                assert topic_name in result["topic"]


class TestGrantPublisherToSink:
    """Test grant_publisher_to_sink function"""

    @pytest.mark.asyncio
    async def test_grant_publisher_success(self, mock_publisher_client, monkeypatch):
        """Test successful IAM permission grant"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"
        topic_name = "gemini-telemetry-topic"

        # Mock IAM policy
        mock_policy = Mock()
        mock_policy.bindings = []
        mock_publisher_client.get_iam_policy.return_value = mock_policy
        mock_publisher_client.set_iam_policy.return_value = mock_policy

        # Mock asyncio.sleep to skip wait
        async def mock_sleep(seconds):
            pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account,
                topic_name=topic_name
            )

            # Verify IAM calls were made
            assert mock_publisher_client.get_iam_policy.called
            assert mock_publisher_client.set_iam_policy.called

            # Verify result
            assert result["service_account"] == sink_service_account
            assert result["role"] == "roles/pubsub.publisher"
            assert result["status"] == "granted"
            assert result["iam_propagation_wait"] == 90

    @pytest.mark.asyncio
    async def test_grant_publisher_already_granted(self, mock_publisher_client, monkeypatch):
        """Test IAM grant when permission already exists"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        # Mock IAM policy with existing binding
        mock_binding = Mock()
        mock_binding.role = "roles/pubsub.publisher"
        mock_binding.members = [f"serviceAccount:{sink_service_account}"]

        mock_policy = Mock()
        mock_policy.bindings = [mock_binding]
        mock_publisher_client.get_iam_policy.return_value = mock_policy
        mock_publisher_client.set_iam_policy.return_value = mock_policy

        # Mock asyncio.sleep
        async def mock_sleep(seconds):
            pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )

            # Should still return success
            assert result["status"] == "granted"

    @pytest.mark.asyncio
    async def test_grant_waits_for_iam_propagation(self, mock_publisher_client, monkeypatch):
        """Test that function waits 90 seconds for IAM propagation"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        mock_policy = Mock()
        mock_policy.bindings = []
        mock_publisher_client.get_iam_policy.return_value = mock_policy

        # Track sleep calls
        sleep_calls = []

        async def mock_sleep(seconds):
            sleep_calls.append(seconds)

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            await pubsub_service.grant_publisher_to_sink(
                project_id=project_id,
                sink_service_account=sink_service_account
            )

            # Verify 90-second wait
            assert 90 in sleep_calls


class TestVerifyTopicExists:
    """Test verify_topic_exists function"""

    @pytest.mark.asyncio
    async def test_topic_exists(self, mock_publisher_client):
        """Test when topic exists"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        mock_topic = Mock()
        mock_publisher_client.get_topic.return_value = mock_topic

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.verify_topic_exists(project_id, topic_name)

            assert result is True

    @pytest.mark.asyncio
    async def test_topic_not_found(self, mock_publisher_client):
        """Test when topic doesn't exist"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        mock_publisher_client.get_topic.side_effect = NotFound("Topic not found")

        with patch('services.pubsub_service.pubsub_v1.PublisherClient', return_value=mock_publisher_client):
            result = await pubsub_service.verify_topic_exists(project_id, topic_name)

            assert result is False


class TestVerifySubscriptionExists:
    """Test verify_subscription_exists function"""

    @pytest.mark.asyncio
    async def test_subscription_exists(self, mock_subscriber_client):
        """Test when subscription exists"""
        project_id = "test-project-123"
        subscription_name = "gemini-telemetry-sub"

        mock_subscription = Mock()
        mock_subscriber_client.get_subscription.return_value = mock_subscription

        with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber_client):
            result = await pubsub_service.verify_subscription_exists(project_id, subscription_name)

            assert result is True

    @pytest.mark.asyncio
    async def test_subscription_not_found(self, mock_subscriber_client):
        """Test when subscription doesn't exist"""
        project_id = "test-project-123"
        subscription_name = "gemini-telemetry-sub"

        mock_subscriber_client.get_subscription.side_effect = NotFound("Subscription not found")

        with patch('services.pubsub_service.pubsub_v1.SubscriberClient', return_value=mock_subscriber_client):
            result = await pubsub_service.verify_subscription_exists(project_id, subscription_name)

            assert result is False


class TestCreatePubsubResources:
    """Test create_pubsub_resources convenience function"""

    @pytest.mark.asyncio
    async def test_creates_all_resources(self, monkeypatch):
        """Test that function creates topic and subscription"""
        project_id = "test-project-123"

        # Track function calls
        topic_created = False
        subscription_created = False

        async def mock_create_topic(pid, topic_name="gemini-telemetry-topic"):
            nonlocal topic_created
            topic_created = True
            return {"topic": f"projects/{pid}/topics/{topic_name}", "status": "created"}

        async def mock_create_subscription(pid, topic_name="gemini-telemetry-topic", subscription_name="gemini-telemetry-sub"):
            nonlocal subscription_created
            subscription_created = True
            return {"subscription": f"projects/{pid}/subscriptions/{subscription_name}", "status": "created"}

        monkeypatch.setattr(pubsub_service, 'create_topic', mock_create_topic)
        monkeypatch.setattr(pubsub_service, 'create_subscription', mock_create_subscription)

        result = await pubsub_service.create_pubsub_resources(project_id)

        # Verify both resources created
        assert topic_created
        assert subscription_created
        assert "topic" in result
        assert "subscription" in result

    @pytest.mark.asyncio
    async def test_grants_permissions_when_sink_provided(self, monkeypatch):
        """Test that function grants permissions when sink service account provided"""
        project_id = "test-project-123"
        sink_service_account = "sink-sa@example.iam.gserviceaccount.com"

        permissions_granted = False

        async def mock_create_topic(pid, topic_name="gemini-telemetry-topic"):
            return {"topic": f"projects/{pid}/topics/{topic_name}", "status": "created"}

        async def mock_create_subscription(pid, topic_name="gemini-telemetry-topic", subscription_name="gemini-telemetry-sub"):
            return {"subscription": f"projects/{pid}/subscriptions/{subscription_name}", "status": "created"}

        async def mock_grant_publisher(project_id, sink_service_account, topic_name="gemini-telemetry-topic"):
            nonlocal permissions_granted
            permissions_granted = True
            return {"status": "granted"}

        monkeypatch.setattr(pubsub_service, 'create_topic', mock_create_topic)
        monkeypatch.setattr(pubsub_service, 'create_subscription', mock_create_subscription)
        monkeypatch.setattr(pubsub_service, 'grant_publisher_to_sink', mock_grant_publisher)

        result = await pubsub_service.create_pubsub_resources(
            project_id=project_id,
            sink_service_account=sink_service_account
        )

        # Verify permissions were granted
        assert permissions_granted
        assert "permissions" in result
