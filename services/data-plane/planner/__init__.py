"""planner — Phase-03 fast-path planning modules (Part E: two-tier planner).

classifier/ decides simple vs. complex; templates/ (Step D) executes the
fast path's template DAG for whatever classifier let through. The
deliberate path (LLM planner, Workflow IR, cost gate) is Phase-06 and does
not live here.
"""
