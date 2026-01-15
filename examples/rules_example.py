#!/usr/bin/env python3
# pyre-strict

"""YieldMonitor CLI - Pipeline monitoring and yield tracking.

Usage:
    buck run fbcode//fblearner/flow/projects/frlpgh/codec_avatar/yield_monitor:run -- \
        --monitor id055 --date 2025-01-12 --output results.json
"""

import argparse
import asyncio
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List


from ..lib import (
    EnricherService,
    JsonReporter,
    PipelineProvider,
    PipelineRule,
    PipelineNode,
    TreeEvaluatorService,
)


logger: logging.Logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="YieldMonitor Framework CLI - Pipeline monitoring and yield tracking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--monitor",
        required=True,
        help="Monitor name (e.g., 'id055', 'id056'). Must match a directory in monitors/",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output.json"),
        help="Output JSON file path (default: manifold://codec-avatar-scratch/tree/sergiu/yield_monitor/{monitor}/output.json)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (debug) logging",
    )

    return parser.parse_args()


async def run_monitor(
    nodes: List[PipelineNode],
    providers: List[PipelineProvider],
    root_rules: List[PipelineRule],
) -> Dict[str, Any]:
    """Execute the full pipeline: enrich → evaluate → report.

    Args:
        nodes: List of PipelineNodes to process.
        providers: List of PipelineProviders for data enrichment.
        root_rules: List of root PipelineRules (entry points of rule trees).

    Returns:
        JSON-serializable dictionary with results and flow summary.
    """
    logger.info(f"Processing {len(nodes)} nodes with {len(providers)} providers")

    # 1. Enrich nodes with provider data
    enricher = EnricherService(providers)
    enriched_nodes = await enricher.execute(nodes)
    logger.info("Data enrichment complete")

    # 2. Evaluate rules
    evaluator = TreeEvaluatorService(root_rules=root_rules)
    eval_results = evaluator.process_all(enriched_nodes)
    logger.info(f"Rule evaluation complete: {len(eval_results)} studies processed")

    # 3. Generate report
    reporter = JsonReporter()
    json_output = reporter.generate(
        enriched_nodes,
        eval_results,
        root_rules=root_rules,
    )

    return json_output


def main() -> int:
    """Main entry point for the YieldMonitor CLI."""
    args = parse_args()

    # Dynamically import the monitor definition
    monitor_module_name = (
        f"fblearner.flow.projects.frlpgh.codec_avatar."
        f"yield_monitor.monitors.{args.monitor}.definition"
    )

    try:
        monitor_module = importlib.import_module(monitor_module_name)
    except ImportError as e:
        logger.error(f"Could not load monitor '{args.monitor}': {e}")
        logger.error(f"Tried to import: {monitor_module_name}")
        return 1

    # Validate required functions
    required_functions = ["load_source_data", "get_providers", "get_rule_tree"]
    for func_name in required_functions:
        if not hasattr(monitor_module, func_name):
            logger.error(
                f"Monitor '{args.monitor}' is missing required function: {func_name}"
            )
            return 1

    # Load source data
    logger.info(f"Loading source data for monitor '{args.monitor}'")
    nodes = monitor_module.load_source_data()
    logger.info(f"Loaded {len(nodes)} nodes")

    if not nodes:
        logger.warning("No nodes loaded. Output will be empty.")

    # Get providers and rules from monitor definition
    providers = monitor_module.get_providers()
    root_rules = monitor_module.get_rule_tree()

    logger.info(
        f"Configured {len(providers)} providers and {len(root_rules)} root rules"
    )

    # Run the pipeline
    result = asyncio.run(run_monitor(nodes, providers, root_rules))

    # Write output
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2, default=str)

    logger.info(f"Results written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
