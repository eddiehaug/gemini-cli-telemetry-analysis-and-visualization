"""
Network service for fetching VPC networks and subnets.

This module provides functions to list available VPC networks and subnets
for a given GCP project and region.
"""

import logging
import subprocess
import json
from typing import Dict, List

logger = logging.getLogger(__name__)


async def list_networks(project_id: str) -> List[Dict]:
    """
    List VPC networks available in the project.

    Args:
        project_id: GCP project ID

    Returns:
        List of network dictionaries with name and selfLink
    """
    try:
        command = [
            "gcloud", "compute", "networks", "list",
            "--project", project_id,
            "--format", "json"
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            logger.error(f"Failed to list networks: {result.stderr}")
            return [{"name": "default", "selfLink": ""}]

        networks = json.loads(result.stdout)

        # Extract just name and selfLink
        network_list = [
            {
                "name": network.get("name"),
                "selfLink": network.get("selfLink", "")
            }
            for network in networks
        ]

        logger.info(f"Found {len(network_list)} networks in project {project_id}")
        return network_list

    except Exception as e:
        logger.error(f"Error listing networks: {str(e)}")
        # Return default network as fallback
        return [{"name": "default", "selfLink": ""}]


async def list_subnets(project_id: str, region: str, network: str = None) -> List[Dict]:
    """
    List subnets available in the project and region.

    Args:
        project_id: GCP project ID
        region: GCP region
        network: Optional network name to filter by

    Returns:
        List of subnet dictionaries with name, network, and region
    """
    try:
        command = [
            "gcloud", "compute", "networks", "subnets", "list",
            "--project", project_id,
            "--filter", f"region:{region}",
            "--format", "json"
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            logger.error(f"Failed to list subnets: {result.stderr}")
            return [{"name": "default", "network": network or "default", "region": region}]

        subnets = json.loads(result.stdout)

        # Filter by network if specified
        if network:
            subnets = [
                subnet for subnet in subnets
                if network in subnet.get("network", "")
            ]

        # Extract relevant fields
        subnet_list = [
            {
                "name": subnet.get("name"),
                "network": subnet.get("network", "").split("/")[-1],
                "region": subnet.get("region", "").split("/")[-1],
                "ipCidrRange": subnet.get("ipCidrRange", "")
            }
            for subnet in subnets
        ]

        logger.info(f"Found {len(subnet_list)} subnets in {region} for project {project_id}")
        return subnet_list

    except Exception as e:
        logger.error(f"Error listing subnets: {str(e)}")
        # Return default subnet as fallback
        return [{"name": "default", "network": network or "default", "region": region}]


async def get_networks_and_subnets(project_id: str, region: str) -> Dict:
    """
    Get both networks and subnets for a project and region.

    This is a convenience function that fetches both in one call.

    Args:
        project_id: GCP project ID
        region: GCP region

    Returns:
        Dict with networks and subnets:
        {
            "networks": [...],
            "subnets": [...]
        }
    """
    try:
        networks = await list_networks(project_id)
        subnets = await list_subnets(project_id, region)

        return {
            "networks": networks,
            "subnets": subnets
        }

    except Exception as e:
        logger.error(f"Error fetching networks and subnets: {str(e)}")
        raise
