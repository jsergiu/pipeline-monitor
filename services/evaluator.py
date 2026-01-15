# pyre-strict

import logging
from typing import Dict, List

from ..core import PipelineRule, RuleResult, SessionNode


logger: logging.Logger = logging.getLogger(__name__)


class TreeEvaluatorService:
    """Evaluates rule trees recursively on node sets with safe error handling.

    The TreeEvaluatorService traverses the rule tree for each node, evaluating
    rules in depth-first order. It propagates pass/fail status to child rules,
    marking children as BLOCKED when their parent fails.

    Key behaviors:
    - Failed parent → children marked as BLOCKED
    - Exceptions in rules → caught, logged as failure, children BLOCKED
    - Results stored as Dict[study_key, Dict[rule_name, RuleResult]]

    Example:
        evaluator = TreeEvaluatorService(root_rules=[scheduled_rule])
        results = evaluator.process_all(enriched_nodes)
        # results["study_ABC123_20250112"]["study_scheduled"].passed == True
    """

    def __init__(self, root_rules: List[PipelineRule]) -> None:
        """Initialize the evaluator with root rules.

        Args:
            root_rules: List of root PipelineRule instances (entry points of rule trees).
        """
        self.root_rules = root_rules

    def process_all(self, nodes: List[SessionNode]) -> Dict[str, Dict[str, RuleResult]]:
        """Process all nodes through all rule trees.

        Args:
            nodes: List of SessionNodes to evaluate.

        Returns:
            Mapping of { study_key: { rule_name: RuleResult } }
        """
        all_results: Dict[str, Dict[str, RuleResult]] = {}

        for node in nodes:
            study_key = f"study_{node.participant_id}_{node.capture_date}"

            node_results: Dict[str, RuleResult] = {}
            for root in self.root_rules:
                self._evaluate_recursive(root, node, node_results, parent_passed=True)

            all_results[study_key] = node_results

        return all_results

    def _evaluate_recursive(
        self,
        rule: PipelineRule,
        node: SessionNode,
        results: Dict[str, RuleResult],
        parent_passed: bool,
    ) -> None:
        """Recursively evaluate a rule and its children.

        Safe tree traversal: exceptions are caught and logged as failures,
        and children are marked as BLOCKED rather than crashing the entire
        evaluation process.

        Args:
            rule: The current rule to evaluate.
            node: The SessionNode to evaluate against.
            results: Dictionary to store results (mutated in place).
            parent_passed: Whether the parent rule passed (False means BLOCKED).
        """
        if not parent_passed:
            # Parent failed → this rule is blocked
            res = RuleResult(
                passed=False,
                name=rule.name,
                message="Blocked by parent failure",
                metadata={"status": "BLOCKED"},
            )
        else:
            try:
                res = rule.evaluate(node)
                # Ensure name is consistent with rule definition
                res.name = rule.name
            except Exception as e:
                # Rule crashed → mark as failure, children will be BLOCKED
                logger.error(f"Rule '{rule.name}' failed with exception: {e}")
                res = RuleResult(
                    passed=False,
                    name=rule.name,
                    message=f"Rule Exception: {e!s}",
                    metadata={"status": "ERROR", "exception": str(e)},
                )

        # Store result
        results[rule.name] = res

        # Recursively process children
        # If this rule failed, force children to be blocked (parent_passed=False)
        for child in rule.children:
            self._evaluate_recursive(child, node, results, parent_passed=res.passed)
