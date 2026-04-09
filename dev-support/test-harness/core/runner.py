"""Test discovery, ordering, and execution (with parallel support)."""

import importlib
import os
import pkgutil
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.assertions import AssertionError, SkipTestError
from core.decorators import get_suite_registry, register_suite_tests
from core.reporter import TestResult


def discover_suites(suites_package="suites"):
    """Import all suites/test_*.py modules to trigger @suite decorators."""
    pkg_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), suites_package)
    for importer, modname, ispkg in pkgutil.iter_modules([pkg_dir]):
        if modname.startswith("test_"):
            importlib.import_module(f"{suites_package}.{modname}")

    # Register tests within each suite
    registry = get_suite_registry()
    for suite_name, meta in registry.items():
        register_suite_tests(suite_name, meta["cls"])


def resolve_order(suite_filter=None):
    """Topological sort of suites by depends_on_suites. Returns ordered list of suite names."""
    registry = get_suite_registry()

    # Filter suites if specified
    if suite_filter:
        # Normalize names: allow "admin" to match "test_admin" or "admin"
        normalized = set()
        for s in suite_filter:
            normalized.add(s)
            normalized.add(f"test_{s}")
            # Strip test_ prefix for matching
            if s.startswith("test_"):
                normalized.add(s[5:])

        available = {name for name in registry if name in normalized or
                     f"test_{name}" in normalized or
                     name.replace("test_", "") in normalized}

        requested = set(available)  # snapshot of explicitly requested suites

        # Include dependencies (recursive)
        def add_deps(name):
            if name in available:
                return
            available.add(name)
            for dep in registry.get(name, {}).get("depends_on", []):
                add_deps(dep)

        for name in list(available):
            for dep in registry.get(name, {}).get("depends_on", []):
                add_deps(dep)

        # Print auto-resolved dependencies so user knows what's happening
        auto_added = available - requested
        if auto_added:
            dep_list = ", ".join(sorted(auto_added))
            print(f"\033[90m[deps] Auto-including dependent suites: {dep_list}\033[0m")
    else:
        available = set(registry.keys())

    # Topological sort (Kahn's algorithm)
    in_degree = {name: 0 for name in available}
    for name in available:
        for dep in registry.get(name, {}).get("depends_on", []):
            if dep in available:
                in_degree[name] = in_degree.get(name, 0) + 1

    queue = sorted([n for n in available if in_degree.get(n, 0) == 0])
    ordered = []
    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for name in available:
            deps = registry.get(name, {}).get("depends_on", [])
            if node in deps:
                in_degree[name] -= 1
                if in_degree[name] == 0:
                    queue.append(name)
        queue.sort()

    # Check for cycles
    if len(ordered) < len(available):
        missing = available - set(ordered)
        print(f"WARNING: Circular dependencies detected for suites: {missing}")
        ordered.extend(sorted(missing))

    return ordered


def _compute_levels(ordered_suites, registry):
    """Group suites into dependency levels for parallel execution.

    Level 0: suites with no dependencies (or all deps already in earlier levels)
    Level 1: suites whose deps are all in level 0
    etc.

    Returns list of lists: [[level0_suites], [level1_suites], ...]
    """
    suite_level = {}
    levels = defaultdict(list)

    for name in ordered_suites:
        deps = registry.get(name, {}).get("depends_on", [])
        if not deps:
            suite_level[name] = 0
        else:
            max_dep_level = max(
                (suite_level.get(d, 0) for d in deps if d in suite_level),
                default=0,
            )
            suite_level[name] = max_dep_level + 1
        levels[suite_level[name]].append(name)

    return [levels[i] for i in sorted(levels.keys())]


def _should_run_test(test_meta, tags, exclude_tags):
    """Check if a test should run based on tag filters."""
    test_tags = set(test_meta.get("tags", []))

    if exclude_tags:
        if test_tags & set(exclude_tags):
            return False

    if tags:
        if not (test_tags & set(tags)):
            return False

    return True


def _run_single_suite(suite_name, registry, client, ctx, config):
    """Execute a single suite silently. Returns (suite_name, results, duration_s).

    Does NOT print anything — results are buffered for ordered output later.
    """
    meta = registry.get(suite_name)
    if not meta:
        return suite_name, [], 0.0

    suite_cls = meta["cls"]
    tests = meta.get("tests", [])

    if not tests:
        return suite_name, [], 0.0

    # Filter tests by tags
    runnable_tests = [
        t for t in tests
        if _should_run_test(t, config.tags, config.exclude_tags)
    ]

    if not runnable_tests:
        return suite_name, [], 0.0

    results = []
    suite_start = time.perf_counter()

    # Instantiate suite
    suite_instance = None
    if isinstance(suite_cls, type):
        suite_instance = suite_cls()

    # Run setup
    if suite_instance and hasattr(suite_instance, "setup"):
        try:
            suite_instance.setup(client, ctx)
        except Exception as e:
            results.append(TestResult(
                suite=suite_name,
                test_name="<setup>",
                status="ERROR",
                error_message=str(e),
                error_type=type(e).__name__,
            ))
            duration = time.perf_counter() - suite_start
            return suite_name, results, duration

    # Run tests
    blocked_tests = set()
    for test_meta in runnable_tests:
        test_name = test_meta["name"]
        test_fn = test_meta["fn"]

        # Check depends_on
        deps = test_meta.get("depends_on", [])
        if any(d in blocked_tests for d in deps):
            blocked_tests.add(test_name)
            results.append(TestResult(
                suite=suite_name,
                test_name=test_name,
                status="SKIP",
                error_message=f"Dependency failed: {deps}",
            ))
            continue

        request_logger = ctx.get("request_logger")
        if request_logger:
            request_logger.set_context(suite_name, test_name)

        start = time.perf_counter()
        try:
            if suite_instance:
                test_fn(suite_instance, client, ctx)
            else:
                test_fn(client, ctx)
            latency = (time.perf_counter() - start) * 1000
            results.append(TestResult(
                suite=suite_name,
                test_name=test_name,
                status="PASS",
                latency_ms=latency,
            ))
        except SkipTestError as e:
            latency = (time.perf_counter() - start) * 1000
            blocked_tests.add(test_name)
            results.append(TestResult(
                suite=suite_name,
                test_name=test_name,
                status="SKIP",
                latency_ms=latency,
                error_message=str(e),
            ))
        except AssertionError as e:
            latency = (time.perf_counter() - start) * 1000
            blocked_tests.add(test_name)
            results.append(TestResult(
                suite=suite_name,
                test_name=test_name,
                status="FAIL",
                latency_ms=latency,
                error_message=str(e),
                error_type="AssertionError",
            ))
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            blocked_tests.add(test_name)
            tb = traceback.format_exc()
            results.append(TestResult(
                suite=suite_name,
                test_name=test_name,
                status="ERROR",
                latency_ms=latency,
                error_message=f"{type(e).__name__}: {e}\n{tb}",
                error_type=type(e).__name__,
            ))

    # Run teardown
    if suite_instance and hasattr(suite_instance, "teardown"):
        try:
            suite_instance.teardown(client, ctx)
        except Exception:
            pass

    duration = time.perf_counter() - suite_start
    return suite_name, results, duration


def run(client, ctx, reporter, config):
    """Main test execution loop. Supports parallel suite execution."""
    discover_suites()
    ordered_suites = resolve_order(config.suites if config.suites else None)

    registry = get_suite_registry()

    if config.verbose:
        print(f"Suite execution order: {ordered_suites}")

    parallel = config.parallel

    if parallel > 1:
        _run_parallel(ordered_suites, registry, client, ctx, reporter, config, parallel)
    else:
        _run_sequential(ordered_suites, registry, client, ctx, reporter, config)


def _run_sequential(ordered_suites, registry, client, ctx, reporter, config):
    """Original sequential execution."""
    for suite_name in ordered_suites:
        meta = registry.get(suite_name)
        if not meta:
            continue

        suite_cls = meta["cls"]
        tests = meta.get("tests", [])

        if not tests:
            continue

        # Filter tests by tags
        runnable_tests = [
            t for t in tests
            if _should_run_test(t, config.tags, config.exclude_tags)
        ]

        if not runnable_tests:
            continue

        reporter.suite_start(suite_name, meta.get("description", ""))

        # Instantiate suite if it's a class with setup/teardown
        suite_instance = None
        if isinstance(suite_cls, type):
            suite_instance = suite_cls()

        # Run setup if available
        if suite_instance and hasattr(suite_instance, "setup"):
            try:
                suite_instance.setup(client, ctx)
            except Exception as e:
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name="<setup>",
                    status="ERROR",
                    error_message=str(e),
                    error_type=type(e).__name__,
                ))
                reporter.suite_end(suite_name)
                continue

        # Run tests
        blocked_tests = set()  # Tests that failed, errored, or were skipped
        for test_meta in runnable_tests:
            test_name = test_meta["name"]
            test_fn = test_meta["fn"]

            # Check depends_on
            deps = test_meta.get("depends_on", [])
            if any(d in blocked_tests for d in deps):
                blocked_tests.add(test_name)
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="SKIP",
                    error_message=f"Dependency failed: {deps}",
                ))
                continue

            request_logger = ctx.get("request_logger")
            if request_logger:
                request_logger.set_context(suite_name, test_name)

            start = time.perf_counter()
            try:
                if suite_instance:
                    test_fn(suite_instance, client, ctx)
                else:
                    test_fn(client, ctx)
                latency = (time.perf_counter() - start) * 1000
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="PASS",
                    latency_ms=latency,
                ))
            except SkipTestError as e:
                latency = (time.perf_counter() - start) * 1000
                blocked_tests.add(test_name)
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="SKIP",
                    latency_ms=latency,
                    error_message=str(e),
                ))
            except AssertionError as e:
                latency = (time.perf_counter() - start) * 1000
                blocked_tests.add(test_name)
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="FAIL",
                    latency_ms=latency,
                    error_message=str(e),
                    error_type="AssertionError",
                ))
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                blocked_tests.add(test_name)
                tb = traceback.format_exc()
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="ERROR",
                    latency_ms=latency,
                    error_message=f"{type(e).__name__}: {e}\n{tb}",
                    error_type=type(e).__name__,
                ))

        # Run teardown if available
        if suite_instance and hasattr(suite_instance, "teardown"):
            try:
                suite_instance.teardown(client, ctx)
            except Exception as e:
                if config.verbose:
                    print(f"  Teardown error in {suite_name}: {e}")

        reporter.suite_end(suite_name)


def _run_parallel(ordered_suites, registry, client, ctx, reporter, config, max_workers):
    """Parallel execution: run independent suites concurrently within each level."""
    levels = _compute_levels(ordered_suites, registry)

    if config.verbose:
        for i, level in enumerate(levels):
            print(f"  Level {i}: {level}")

    for level_idx, level_suites in enumerate(levels):
        # Filter to suites that have runnable tests
        runnable_in_level = []
        for name in level_suites:
            meta = registry.get(name)
            if not meta:
                continue
            tests = meta.get("tests", [])
            if not tests:
                continue
            runnable = [t for t in tests if _should_run_test(t, config.tags, config.exclude_tags)]
            if runnable:
                runnable_in_level.append(name)

        if not runnable_in_level:
            continue

        if len(runnable_in_level) == 1:
            # Single suite in level — run sequentially (skip thread overhead)
            _run_sequential(runnable_in_level, registry, client, ctx, reporter, config)
            continue

        # Multiple suites — run in parallel
        level_start = time.perf_counter()
        workers = min(max_workers, len(runnable_in_level))

        # Count total tests across all suites in this level
        total_tests = 0
        for name in runnable_in_level:
            meta = registry.get(name, {})
            tests = meta.get("tests", [])
            total_tests += len([t for t in tests
                                if _should_run_test(t, config.tags, config.exclude_tags)])

        suite_list = ", ".join(runnable_in_level)
        print(
            f"\n\033[96m\033[1m[Parallel] Running {len(runnable_in_level)} suites "
            f"({total_tests} tests, {workers} workers): "
            f"{suite_list}\033[0m",
            flush=True,
        )

        completed_count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _run_single_suite, name, registry, client, ctx, config,
                ): name
                for name in runnable_in_level
            }
            for future in as_completed(futures):
                name = futures[future]
                completed_count += 1
                try:
                    suite_name, results, duration = future.result()
                except Exception as e:
                    suite_name = name
                    results = [TestResult(
                        suite=name,
                        test_name="<parallel_error>",
                        status="ERROR",
                        error_message=f"Suite thread error: {e}",
                        error_type=type(e).__name__,
                    )]
                    duration = 0.0

                # Print immediately as each suite completes
                meta = registry.get(suite_name, {})
                remaining = len(runnable_in_level) - completed_count
                reporter.suite_start(
                    suite_name,
                    meta.get("description", ""),
                )
                reporter.replay_results(results)
                reporter.suite_end(suite_name)
                if remaining > 0:
                    print(
                        f"    \033[90m({remaining} suite{'s' if remaining != 1 else ''} "
                        f"still running...)\033[0m",
                        flush=True,
                    )

        level_duration = time.perf_counter() - level_start
        print(
            f"\033[96m[Parallel] Level completed in {level_duration:.1f}s\033[0m",
            flush=True,
        )
