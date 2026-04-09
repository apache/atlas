"""Test and suite registration decorators."""

from typing import Callable, Dict, List, Optional

# Global registries
_test_registry: Dict[str, Dict] = {}     # "suite::test_name" -> metadata
_suite_registry: Dict[str, Dict] = {}    # suite_name -> metadata


def suite(name, depends_on_suites=None, description=""):
    """Decorator to register a test suite module.

    Usage:
        @suite("entity_crud", depends_on_suites=["typedefs"])
        class EntityCrudSuite:
            ...
    """
    def decorator(cls):
        _suite_registry[name] = {
            "name": name,
            "cls": cls,
            "depends_on": depends_on_suites or [],
            "description": description,
            "tests": [],
        }
        cls._suite_name = name
        return cls
    return decorator


def test(name=None, tags=None, depends_on=None, order=100):
    """Decorator to register a test function within a suite.

    Usage:
        @test("create_entity", tags=["smoke", "crud"], order=10)
        def test_create_entity(client, ctx):
            ...
    """
    def decorator(fn):
        test_name = name or fn.__name__
        fn._test_meta = {
            "name": test_name,
            "tags": tags or [],
            "depends_on": depends_on or [],
            "order": order,
            "fn": fn,
        }
        return fn
    return decorator


def get_suite_registry() -> Dict[str, Dict]:
    return _suite_registry


def get_test_registry() -> Dict[str, Dict]:
    return _test_registry


def register_suite_tests(suite_name, suite_cls):
    """Scan a suite class for @test-decorated methods and register them."""
    tests = []
    for attr_name in dir(suite_cls):
        attr = getattr(suite_cls, attr_name, None)
        if callable(attr) and hasattr(attr, "_test_meta"):
            meta = attr._test_meta
            key = f"{suite_name}::{meta['name']}"
            _test_registry[key] = {**meta, "suite": suite_name}
            tests.append(meta)
    tests.sort(key=lambda t: t["order"])
    if suite_name in _suite_registry:
        _suite_registry[suite_name]["tests"] = tests
    return tests
