import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Mapping, Optional

from grammar_types import load_grammar_types_yaml


class TransformProcessRuleMode(str, Enum):
    MANUAL = "manual"
    MANUAL_REDUCE = "manual_reduce"
    FORWARD = "forward"
    EXCLUDED = "excluded"


@dataclass(frozen=True)
class TransformProcessRuleConfig:
    rule_name: str
    mode: TransformProcessRuleMode
    prepare: Optional[str] = None
    reduce: Optional[str] = None


def _fail(config_file: Path, errors):
    print(
        f"Error: {config_file} contains invalid process transformer metadata:",
        file=sys.stderr,
    )
    for error in errors:
        print(error, file=sys.stderr)
    sys.exit(1)


def load_transformer_process_config(
    config_file: Path, known_rules, require_known_rules: bool = True
) -> Dict[str, TransformProcessRuleConfig]:
    data = load_grammar_types_yaml(config_file)
    rules = data.get("rules", {})
    errors = []

    if not isinstance(rules, Mapping):
        errors.append("top-level 'rules' entry must be a mapping")
        _fail(config_file, errors)

    result = {}
    known_rule_set = set(known_rules)
    for rule_name, entry in rules.items():
        if not isinstance(entry, Mapping):
            errors.append(f"rule '{rule_name}' must use a mapping")
            continue
        if require_known_rules and rule_name not in known_rule_set:
            errors.append(f"rule '{rule_name}' does not exist in the grammar")
            continue

        mode_value = entry.get("mode")
        prepare = entry.get("prepare")
        reduce = entry.get("reduce")

        try:
            mode = TransformProcessRuleMode(mode_value)
        except ValueError:
            valid_modes = ", ".join(mode.value for mode in TransformProcessRuleMode)
            errors.append(f"rule '{rule_name}' has invalid mode '{mode_value}' (expected one of: {valid_modes})")
            continue

        if prepare is not None and not isinstance(prepare, str):
            errors.append(f"rule '{rule_name}' has non-string prepare hook")
        if reduce is not None and not isinstance(reduce, str):
            errors.append(f"rule '{rule_name}' has non-string reduce hook")

        if mode == TransformProcessRuleMode.MANUAL:
            if not prepare:
                errors.append(f"manual rule '{rule_name}' must declare a prepare hook")
            if not reduce:
                errors.append(f"manual rule '{rule_name}' must declare a reduce hook")
        elif mode == TransformProcessRuleMode.MANUAL_REDUCE:
            if prepare:
                errors.append(f"manual_reduce rule '{rule_name}' must not declare a prepare hook")
            if not reduce:
                errors.append(f"manual_reduce rule '{rule_name}' must declare a reduce hook")
        elif mode == TransformProcessRuleMode.FORWARD:
            if prepare or reduce:
                errors.append(f"forward rule '{rule_name}' must not declare hooks")
        elif mode == TransformProcessRuleMode.EXCLUDED:
            if prepare or reduce:
                errors.append(f"excluded rule '{rule_name}' must not declare hooks")

        result[rule_name] = TransformProcessRuleConfig(
            rule_name=str(rule_name), mode=mode, prepare=prepare, reduce=reduce
        )

    if errors:
        _fail(config_file, errors)

    return result
