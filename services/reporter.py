# pyre-strict

from typing import Any, Dict, List

from ..core import PipelineRule, RuleResult, SessionNode


class JsonReporter:
    """Generates JSON reports from evaluation results with Sankey flow data.

    The JsonReporter produces a structured JSON output containing:
    1. Per-node results with metadata, errors, and pipeline step statuses
    2. Flow summary with Sankey diagram links showing node transitions

    Example output:
        {
            "results": {
                "study_ABC123_20250112": {
                    "date": "20250112",
                    "time": "1430",
                    "participant_id": "ABC123",
                    "datalake": {...},
                    "data_hub": {...},
                    "errors": [],
                    "pipeline_steps": {
                        "study_scheduled": {"passed": true, "message": ""}
                    }
                }
            },
            "flow_summary": {
                "sankey_links": [
                    {"source": "step_a", "target": "step_b", "value": 10}
                ]
            }
        }
    """

    def generate(
        self,
        nodes: List[SessionNode],
        evaluation_results: Dict[str, Dict[str, RuleResult]],
        root_rules: List[PipelineRule] | None = None,
    ) -> Dict[str, Any]:
        """Generate complete JSON output with per-node results and flow summary.

        Args:
            nodes: List of SessionNodes that were evaluated.
            evaluation_results: Results from TreeEvaluatorService.process_all().
            root_rules: Optional list of root rules for Sankey flow calculation.

        Returns:
            Dictionary ready for JSON serialization.
        """
        output: Dict[str, Any] = {"results": {}}

        # 1. Generate Per-Node Results
        for node in nodes:
            study_key = f"study_{node.participant_id}_{node.capture_date}"

            node_data: Dict[str, Any] = {
                "date": node.capture_date,
                "time": node.capture_time,
                "participant_id": node.participant_id,
                "datalake": node.metadata.get("datalake", {}),
                "data_hub": node.metadata.get("datahub", {}),
                "errors": getattr(node, "errors", []),
            }

            # Add pipeline step results
            steps: Dict[str, Any] = {}
            node_results = evaluation_results.get(study_key, {})
            for rule_name, res in node_results.items():
                steps[rule_name] = {
                    "passed": res.passed,
                    "message": res.message,
                    **res.metadata,
                }

            node_data["pipeline_steps"] = steps
            output["results"][study_key] = node_data

        # 2. Generate Flow Summary (Sankey Links)
        if root_rules:
            output["flow_summary"] = self._calculate_flows(
                root_rules, evaluation_results
            )

        return output

    def _calculate_flows(
        self,
        roots: List[PipelineRule],
        all_results: Dict[str, Dict[str, RuleResult]],
    ) -> Dict[str, Any]:
        """Calculate Sankey diagram links from rule tree and results.

        Creates links between connected rules, counting how many nodes
        successfully passed from parent to child.

        Args:
            roots: List of root PipelineRule instances.
            all_results: Evaluation results from TreeEvaluatorService.

        Returns:
            Dictionary with "sankey_links" key containing list of link objects.
        """
        links: List[Dict[str, Any]] = []

        def traverse(rule: PipelineRule) -> None:
            for child in rule.children:
                count = 0
                for res_map in all_results.values():
                    parent_res = res_map.get(rule.name)
                    child_res = res_map.get(child.name)
                    # A flow exists if BOTH parent and child passed
                    if (
                        parent_res
                        and parent_res.passed
                        and child_res
                        and child_res.passed
                    ):
                        count += 1

                if count > 0:
                    links.append(
                        {
                            "source": rule.name,
                            "target": child.name,
                            "value": count,
                        }
                    )

                traverse(child)

        for root in roots:
            # Add root node count (nodes that passed the root rule)
            root_passed = sum(
                1
                for res_map in all_results.values()
                if res_map.get(root.name) and res_map[root.name].passed
            )
            if root_passed > 0:
                links.insert(
                    0,
                    {
                        "source": "_start",
                        "target": root.name,
                        "value": root_passed,
                    },
                )
            traverse(root)

        return {"sankey_links": links}
