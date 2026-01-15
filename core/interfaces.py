# pyre-strict

from abc import ABC, abstractmethod
from typing import List

from .models import RuleResult, SessionNode


class PipelineProvider(ABC):
    """Base interface for all data source providers.

    Providers are responsible for enriching SessionNodes with data from
    external sources (DataLake, Hub, Manifold, etc.). They run concurrently
    via asyncio.gather and should handle errors gracefully per-node.

    Example:
        class MyProvider(PipelineProvider):
            @property
            def name(self) -> str:
                return "MyProvider"

            async def apply(self, nodes: List[SessionNode]) -> List[SessionNode]:
                for node in nodes:
                    try:
                        data = await self.fetch_data(node)
                        node.metadata["my_data"] = data
                    except Exception as e:
                        node.errors.append({"source": self.name, "error": str(e)})
                return nodes
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider identifier for error tracking and debugging."""
        pass

    @abstractmethod
    async def apply(self, nodes: List[SessionNode]) -> List[SessionNode]:
        """Process nodes and enrich them with source-specific data.

        This method is called by the EnricherService and should:
        1. Fetch data from the external source for each node
        2. Store enriched data in node.metadata[key]
        3. Handle errors per-node by appending to node.errors
        4. Return the (mutated) list of nodes

        Args:
            nodes: List of SessionNodes to enrich.

        Returns:
            The same list of nodes with enriched metadata.

        Note:
            - Must be async for concurrent execution
            - Should NOT raise exceptions; handle errors per-node instead
            - Append errors to node.errors rather than crashing
        """
        pass


class PipelineRule(ABC):
    """Base class for pipeline rules with tree composition.

    Rules form a tree structure that is evaluated against each node.
    Use `add_children()` to wire rules together in a flat, readable pattern.

    The "Flat Wiring" Pattern:
        Instantiate rules first, then wire them with add_children().
        This keeps code linear and avoids nesting hell.

    Example:
        # 1. Instantiate Rules (Define the 'What')
        scheduled = ScheduledRule()
        executed = ExecutedRule()
        raw_data = RawDataRule()
        inference = InferenceRule()
        reporting = ReportingRule()

        # 2. Wire the Tree (Define the 'Flow')
        scheduled.add_children([executed])
        executed.add_children([raw_data])
        raw_data.add_children([inference, reporting])  # Branching point

        # 3. Pass only the root to the evaluator
        evaluator = TreeEvaluatorService(root_rules=[scheduled])
    """

    def __init__(self) -> None:
        self.children: List["PipelineRule"] = []

    @property
    @abstractmethod
    def name(self) -> str:
        """The key name in the final JSON output."""
        pass

    @abstractmethod
    def evaluate(self, node: SessionNode) -> RuleResult:
        """Evaluate this rule against a single node.

        Args:
            node: The SessionNode to evaluate.

        Returns:
            RuleResult indicating pass/fail with optional message and metadata.
        """
        pass

    def add_children(self, rules: List["PipelineRule"]) -> "PipelineRule":
        """Add child nodes to this rule.

        Returns SELF (the parent) to allow for nested definitions if desired.

        Args:
            rules: List of child rules to add.

        Returns:
            This rule (self) to allow method chaining.
        """
        self.children.extend(rules)
        return self
