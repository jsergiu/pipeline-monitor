# pyre-strict

import asyncio
import logging
from typing import List

from ..core import PipelineProvider, SessionNode


logger: logging.Logger = logging.getLogger(__name__)


class EnricherService:
    """Runs providers concurrently to enrich nodes with metadata.

    The EnricherService orchestrates multiple PipelineProviders, executing them
    concurrently using asyncio.gather. Each provider enriches nodes with data
    from external sources (DataLake, Hub, Manifold, etc.).

    Example:
        service = EnricherService([
            DataLakeProvider(client=dl_client, dataset_tag="ID055"),
            DataHubProvider(client=hub_client, dataset_tag="ID055"),
        ])
        enriched_nodes = await service.execute(nodes)
    """

    def __init__(self, providers: List[PipelineProvider]) -> None:
        """Initialize the EnricherService with a list of providers.

        Args:
            providers: List of PipelineProvider instances to run.
        """
        self.providers = providers

    async def execute(self, nodes: List[SessionNode]) -> List[SessionNode]:
        """Execute all providers concurrently to enrich nodes.

        All providers run in parallel via asyncio.gather. Each provider
        mutates the nodes in-place by adding data to node.metadata and
        errors to node.errors.

        Args:
            nodes: List of SessionNodes to enrich.

        Returns:
            The same list of nodes with enriched metadata from all providers.

        Note:
            - Provider exceptions are caught and logged, not propagated
            - Individual node errors are stored in node.errors by providers
        """
        if not self.providers:
            return nodes

        if not nodes:
            return nodes

        # Create tasks for all providers
        tasks = [
            self._run_provider_safe(provider, nodes) for provider in self.providers
        ]

        # Run all providers concurrently
        await asyncio.gather(*tasks)

        return nodes

    async def _run_provider_safe(
        self, provider: PipelineProvider, nodes: List[SessionNode]
    ) -> None:
        """Run a single provider with error handling.

        Catches and logs any exceptions from the provider to prevent
        one failing provider from crashing the entire enrichment process.

        Args:
            provider: The provider to run.
            nodes: The nodes to enrich.
        """
        try:
            await provider.apply(nodes)
            logger.debug(f"Provider '{provider.name}' completed successfully")
        except Exception as e:
            logger.error(f"Provider '{provider.name}' failed with error: {e}")
            # Add error to all nodes since we don't know which ones failed
            for node in nodes:
                errors = getattr(node, "errors", None)
                if errors is not None:
                    errors.append(
                        {
                            "source": provider.name,
                            "error": f"Provider failed: {e!s}",
                            "type": "provider_crash",
                        }
                    )
