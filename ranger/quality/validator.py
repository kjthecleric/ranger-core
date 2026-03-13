"""Rule-based data validation engine for Ranger pipelines.

Validates records against configurable rules and classifies violations
by action (reject, flag, warn).
"""

from __future__ import annotations

import re
from typing import Any, Callable

import structlog

from ranger.core.models import Record, ValidationAction

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Rule type constants
# ---------------------------------------------------------------------------

RULE_NOT_NULL = "not_null"
RULE_UNIQUE = "unique"
RULE_RANGE = "range"
RULE_REGEX = "regex"
RULE_ENUM = "enum"
RULE_CUSTOM = "custom"

_VALID_RULE_TYPES: set[str] = {RULE_NOT_NULL, RULE_UNIQUE, RULE_RANGE, RULE_REGEX, RULE_ENUM, RULE_CUSTOM}


# ---------------------------------------------------------------------------
# Violation model
# ---------------------------------------------------------------------------


class Violation:
    """A single validation violation on a record."""

    __slots__ = ("rule_type", "column", "message", "action", "value")

    def __init__(
        self,
        rule_type: str,
        column: str,
        message: str,
        action: ValidationAction,
        value: Any = None,
    ) -> None:
        self.rule_type = rule_type
        self.column = column
        self.message = message
        self.action = action
        self.value = value

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_type": self.rule_type,
            "column": self.column,
            "message": self.message,
            "action": self.action.value,
            "value": self.value,
        }

    def __repr__(self) -> str:
        return f"Violation({self.rule_type}, {self.column!r}, {self.message!r})"


# ---------------------------------------------------------------------------
# Rule definition
# ---------------------------------------------------------------------------


class ValidationRule:
    """A single validation rule definition.

    Attributes:
        type: Rule type — ``not_null``, ``unique``, ``range``, ``regex``, ``enum``, ``custom``.
        columns: List of column names this rule applies to.
        params: Type-specific parameters (e.g. ``min``, ``max``, ``pattern``, ``values``).
        action: What to do on violation — ``reject``, ``warn``, or ``fail``.
    """

    def __init__(
        self,
        type: str,  # noqa: A002 — shadows builtin intentionally for config compatibility
        columns: list[str],
        params: dict[str, Any] | None = None,
        action: str | ValidationAction = ValidationAction.WARN,
    ) -> None:
        if type not in _VALID_RULE_TYPES:
            raise ValueError(f"Unknown rule type: {type!r}. Valid: {_VALID_RULE_TYPES}")
        self.type = type
        self.columns = columns
        self.params = params or {}
        self.action = ValidationAction(action) if isinstance(action, str) else action

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ValidationRule:
        """Construct a rule from a dict (e.g. YAML config)."""
        return cls(
            type=data["type"],
            columns=data.get("columns", []),
            params=data.get("params", {}),
            action=data.get("action", "warn"),
        )


# ---------------------------------------------------------------------------
# DataValidator
# ---------------------------------------------------------------------------


class DataValidator:
    """Rule-based validation engine.

    Validates individual records or entire batches against a list of
    :class:`ValidationRule` definitions.

    Usage::

        rules = [
            ValidationRule("not_null", ["id", "name"], action="reject"),
            ValidationRule("range", ["age"], params={"min": 0, "max": 150}, action="warn"),
            ValidationRule("regex", ["email"], params={"pattern": r".+@.+"}, action="reject"),
            ValidationRule("enum", ["status"], params={"values": ["active", "inactive"]}),
        ]
        validator = DataValidator(rules)
        valid, rejected, warnings = validator.validate_batch(records)
    """

    def __init__(
        self,
        rules: list[ValidationRule] | list[dict[str, Any]] | None = None,
        custom_functions: dict[str, Callable[..., bool]] | None = None,
    ) -> None:
        self._rules: list[ValidationRule] = []
        if rules:
            for r in rules:
                if isinstance(r, dict):
                    self._rules.append(ValidationRule.from_dict(r))
                else:
                    self._rules.append(r)

        # Registry of named custom validator functions
        self._custom_functions: dict[str, Callable[..., bool]] = custom_functions or {}

        # Compiled regex cache
        self._regex_cache: dict[str, re.Pattern[str]] = {}

        # Unique-rule tracking (reset per batch)
        self._unique_seen: dict[str, set[Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_batch(
        self,
        records: list[Record],
        rules: list[ValidationRule] | None = None,
    ) -> tuple[list[Record], list[Record], list[Violation]]:
        """Validate a batch of records.

        Args:
            records: Records to validate.
            rules: Optional override rules (uses instance rules if ``None``).

        Returns:
            Tuple of ``(valid_records, rejected_records, warnings)``.
        """
        active_rules = rules or self._rules
        valid: list[Record] = []
        rejected: list[Record] = []
        warnings: list[Violation] = []

        # Reset unique tracking for this batch
        self._unique_seen = {}

        for record in records:
            is_valid, violations = self.validate_record(record, active_rules)

            # Separate warnings from rejections
            record_warnings = [v for v in violations if v.action == ValidationAction.WARN]
            record_rejects = [v for v in violations if v.action in (ValidationAction.REJECT, ValidationAction.FAIL)]

            warnings.extend(record_warnings)

            if record_rejects:
                rejected.append(record)
                logger.debug(
                    "record_rejected",
                    violations=[v.to_dict() for v in record_rejects],
                )
            else:
                valid.append(record)

        logger.info(
            "batch_validated",
            total=len(records),
            valid=len(valid),
            rejected=len(rejected),
            warnings=len(warnings),
        )

        return valid, rejected, warnings

    def validate_record(
        self,
        record: Record,
        rules: list[ValidationRule] | None = None,
    ) -> tuple[bool, list[Violation]]:
        """Validate a single record against rules.

        Args:
            record: The record to validate.
            rules: Optional override rules.

        Returns:
            Tuple of ``(is_valid, violations)``.
            ``is_valid`` is ``False`` if any violation has action ``reject`` or ``fail``.
        """
        active_rules = rules or self._rules
        violations: list[Violation] = []

        for rule in active_rules:
            rule_violations = self._apply_rule(record, rule)
            violations.extend(rule_violations)

        is_valid = not any(
            v.action in (ValidationAction.REJECT, ValidationAction.FAIL)
            for v in violations
        )
        return is_valid, violations

    def register_custom(self, name: str, func: Callable[..., bool]) -> None:
        """Register a named custom validation function.

        The function receives ``(value, record, params)`` and returns ``True``
        if the value is valid.
        """
        self._custom_functions[name] = func

    # ------------------------------------------------------------------
    # Rule application
    # ------------------------------------------------------------------

    def _apply_rule(self, record: Record, rule: ValidationRule) -> list[Violation]:
        """Dispatch to the appropriate rule checker."""
        handler = {
            RULE_NOT_NULL: self._check_not_null,
            RULE_UNIQUE: self._check_unique,
            RULE_RANGE: self._check_range,
            RULE_REGEX: self._check_regex,
            RULE_ENUM: self._check_enum,
            RULE_CUSTOM: self._check_custom,
        }.get(rule.type)

        if handler is None:
            logger.warning("unknown_rule_type", rule_type=rule.type)
            return []

        violations: list[Violation] = []
        for col in rule.columns:
            violation = handler(record, col, rule)
            if violation:
                violations.append(violation)
        return violations

    def _check_not_null(
        self, record: Record, column: str, rule: ValidationRule
    ) -> Violation | None:
        value = record.data.get(column)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return Violation(
                rule_type=RULE_NOT_NULL,
                column=column,
                message=f"Column '{column}' must not be null or empty",
                action=rule.action,
                value=value,
            )
        return None

    def _check_unique(
        self, record: Record, column: str, rule: ValidationRule
    ) -> Violation | None:
        value = record.data.get(column)
        if column not in self._unique_seen:
            self._unique_seen[column] = set()

        if value in self._unique_seen[column]:
            return Violation(
                rule_type=RULE_UNIQUE,
                column=column,
                message=f"Duplicate value '{value}' in column '{column}'",
                action=rule.action,
                value=value,
            )
        self._unique_seen[column].add(value)
        return None

    def _check_range(
        self, record: Record, column: str, rule: ValidationRule
    ) -> Violation | None:
        value = record.data.get(column)
        if value is None:
            return None  # null handling is done by not_null rule

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return Violation(
                rule_type=RULE_RANGE,
                column=column,
                message=f"Column '{column}' value '{value}' is not numeric",
                action=rule.action,
                value=value,
            )

        min_val = rule.params.get("min")
        max_val = rule.params.get("max")

        if min_val is not None and numeric_value < float(min_val):
            return Violation(
                rule_type=RULE_RANGE,
                column=column,
                message=f"Column '{column}' value {numeric_value} < min {min_val}",
                action=rule.action,
                value=value,
            )
        if max_val is not None and numeric_value > float(max_val):
            return Violation(
                rule_type=RULE_RANGE,
                column=column,
                message=f"Column '{column}' value {numeric_value} > max {max_val}",
                action=rule.action,
                value=value,
            )
        return None

    def _check_regex(
        self, record: Record, column: str, rule: ValidationRule
    ) -> Violation | None:
        value = record.data.get(column)
        if value is None:
            return None

        pattern_str = rule.params.get("pattern", "")
        if not pattern_str:
            return None

        if pattern_str not in self._regex_cache:
            self._regex_cache[pattern_str] = re.compile(pattern_str)

        pattern = self._regex_cache[pattern_str]
        if not pattern.fullmatch(str(value)):
            return Violation(
                rule_type=RULE_REGEX,
                column=column,
                message=f"Column '{column}' value '{value}' does not match pattern '{pattern_str}'",
                action=rule.action,
                value=value,
            )
        return None

    def _check_enum(
        self, record: Record, column: str, rule: ValidationRule
    ) -> Violation | None:
        value = record.data.get(column)
        if value is None:
            return None

        allowed_values = rule.params.get("values", [])
        if value not in allowed_values:
            return Violation(
                rule_type=RULE_ENUM,
                column=column,
                message=f"Column '{column}' value '{value}' not in allowed values: {allowed_values}",
                action=rule.action,
                value=value,
            )
        return None

    def _check_custom(
        self, record: Record, column: str, rule: ValidationRule
    ) -> Violation | None:
        func_name = rule.params.get("function")
        func = rule.params.get("callable")

        if func_name and func_name in self._custom_functions:
            func = self._custom_functions[func_name]

        if func is None:
            logger.warning("custom_rule_no_function", column=column, params=rule.params)
            return None

        value = record.data.get(column)
        try:
            is_valid = func(value, record, rule.params)
        except Exception as exc:
            return Violation(
                rule_type=RULE_CUSTOM,
                column=column,
                message=f"Custom validation error on '{column}': {exc}",
                action=rule.action,
                value=value,
            )

        if not is_valid:
            return Violation(
                rule_type=RULE_CUSTOM,
                column=column,
                message=f"Custom validation failed for column '{column}'",
                action=rule.action,
                value=value,
            )
        return None
