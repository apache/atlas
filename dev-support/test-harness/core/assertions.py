"""Assertion helpers for API response validation."""


class SkipTestError(Exception):
    """Raised to skip a test with an explicit reason."""
    pass


class AssertionError(Exception):
    """Test assertion failure with context."""

    def __init__(self, message, expected=None, actual=None, response=None):
        self.expected = expected
        self.actual = actual
        self.response = response
        detail = message
        if expected is not None:
            detail += f" (expected={expected!r}, actual={actual!r})"
        if response is not None:
            detail += f" [HTTP {response.status_code} {response.request_method} {response.request_path}]"
            # Include truncated response body for debugging
            body = response.body if hasattr(response, "body") else None
            if body and response.status_code >= 400:
                body_str = str(body)[:300]
                detail += f"\n    Response: {body_str}"
        super().__init__(detail)


def assert_status(resp, expected):
    """Assert HTTP status code matches exactly."""
    if resp.status_code != expected:
        raise AssertionError(
            f"Expected status {expected}, got {resp.status_code}",
            expected=expected, actual=resp.status_code, response=resp,
        )


def assert_status_in(resp, expected_list):
    """Assert HTTP status code is one of the expected values."""
    if resp.status_code not in expected_list:
        raise AssertionError(
            f"Expected status in {expected_list}, got {resp.status_code}",
            expected=expected_list, actual=resp.status_code, response=resp,
        )


def _resolve_path(obj, dot_path):
    """Resolve a dot-separated path like 'entity.guid' against a dict/object."""
    parts = dot_path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                return None, False
            current = current[part]
        elif isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx]
            except (ValueError, IndexError):
                return None, False
        else:
            return None, False
    return current, True


def assert_field_present(resp, *dot_path_parts):
    """Assert that a nested field exists in the response body.

    Usage: assert_field_present(resp, "entity", "guid")
           or assert_field_present(resp, "entity.guid")
    """
    dot_path = ".".join(dot_path_parts)
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found in response",
            expected=f"field '{dot_path}' present", actual="missing", response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_equals(resp, dot_path, expected_value):
    """Assert that a nested field equals the expected value."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=expected_value, actual="<missing>", response=resp if hasattr(resp, "status_code") else None,
        )
    if val != expected_value:
        raise AssertionError(
            f"Field '{dot_path}' mismatch",
            expected=expected_value, actual=val, response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_not_empty(resp, dot_path):
    """Assert that a nested field exists and is non-empty (truthy)."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected="non-empty value", actual="<missing>", response=resp if hasattr(resp, "status_code") else None,
        )
    if not val:
        raise AssertionError(
            f"Field '{dot_path}' is empty",
            expected="non-empty value", actual=val, response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_contains(resp, dot_path, substring):
    """Assert that a field's string value contains a substring."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"contains '{substring}'", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if substring not in str(val):
        raise AssertionError(
            f"Field '{dot_path}' does not contain '{substring}'",
            expected=f"contains '{substring}'", actual=val,
            response=resp if hasattr(resp, "status_code") else None,
        )


def assert_list_min_length(resp, dot_path, min_len):
    """Assert that a list field has at least min_len items."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"list with >= {min_len} items", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if not isinstance(val, list):
        raise AssertionError(
            f"Field '{dot_path}' is not a list",
            expected=f"list with >= {min_len} items", actual=type(val).__name__,
            response=resp if hasattr(resp, "status_code") else None,
        )
    if len(val) < min_len:
        raise AssertionError(
            f"Field '{dot_path}' has {len(val)} items, expected >= {min_len}",
            expected=f">= {min_len}", actual=len(val),
            response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_type(resp, dot_path, expected_type):
    """Assert that a nested field exists and is an instance of the expected type."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"type {expected_type.__name__}", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if not isinstance(val, expected_type):
        raise AssertionError(
            f"Field '{dot_path}' has type {type(val).__name__}, expected {expected_type.__name__}",
            expected=expected_type.__name__, actual=type(val).__name__,
            response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_in(resp, dot_path, allowed_values):
    """Assert that a nested field's value is in a set of allowed values."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"one of {allowed_values}", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if val not in allowed_values:
        raise AssertionError(
            f"Field '{dot_path}' value {val!r} not in allowed values",
            expected=allowed_values, actual=val,
            response=resp if hasattr(resp, "status_code") else None,
        )


def assert_list_contains_field(resp, dot_path, field_name, expected_value):
    """Assert that a list at dot_path has at least one item where item[field_name] == expected_value."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"list with {field_name}={expected_value!r}", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if not isinstance(val, list):
        raise AssertionError(
            f"Field '{dot_path}' is not a list",
            expected=f"list with {field_name}={expected_value!r}", actual=type(val).__name__,
            response=resp if hasattr(resp, "status_code") else None,
        )
    found_item = any(
        isinstance(item, dict) and item.get(field_name) == expected_value
        for item in val
    )
    if not found_item:
        raise AssertionError(
            f"No item in '{dot_path}' has {field_name}={expected_value!r}",
            expected=expected_value, actual=[item.get(field_name) for item in val if isinstance(item, dict)],
            response=resp if hasattr(resp, "status_code") else None,
        )


def assert_list_length(body, expected_len, msg=None):
    """Assert a list has exactly expected_len items."""
    if not isinstance(body, list):
        raise AssertionError(
            msg or f"Expected a list, got {type(body).__name__}",
            expected=f"list of length {expected_len}", actual=type(body).__name__,
        )
    if len(body) != expected_len:
        raise AssertionError(
            msg or f"Expected list of length {expected_len}, got {len(body)}",
            expected=expected_len, actual=len(body),
        )


def assert_list_not_empty(body, msg=None):
    """Assert a list is non-empty."""
    if not isinstance(body, list):
        raise AssertionError(
            msg or f"Expected a list, got {type(body).__name__}",
            expected="non-empty list", actual=type(body).__name__,
        )
    if len(body) == 0:
        raise AssertionError(
            msg or "Expected non-empty list, got empty list",
            expected="non-empty list", actual="[]",
        )


def assert_mutation_response(resp, expected_action, type_name=None):
    """Validate mutatedEntities.{action} is non-empty, every entity has guid.

    Returns the entity list from the mutation response.
    """
    body = resp.body if hasattr(resp, "body") else resp
    mutated = body.get("mutatedEntities", {}) if isinstance(body, dict) else {}
    entities = mutated.get(expected_action, [])
    if not entities:
        raise AssertionError(
            f"Expected non-empty mutatedEntities.{expected_action}",
            expected=f"non-empty {expected_action}", actual="empty or missing",
            response=resp if hasattr(resp, "status_code") else None,
        )
    for i, entity in enumerate(entities):
        if not isinstance(entity, dict) or "guid" not in entity:
            raise AssertionError(
                f"Entity [{i}] in mutatedEntities.{expected_action} missing 'guid'",
                expected="guid present", actual=str(entity)[:100],
                response=resp if hasattr(resp, "status_code") else None,
            )
        if type_name and entity.get("typeName") != type_name:
            raise AssertionError(
                f"Entity [{i}] typeName mismatch",
                expected=type_name, actual=entity.get("typeName"),
                response=resp if hasattr(resp, "status_code") else None,
            )
    return entities
