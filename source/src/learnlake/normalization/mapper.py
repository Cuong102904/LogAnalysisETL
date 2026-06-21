from __future__ import annotations

from typing import Any

from learnlake.contracts import FieldMapping, MappingSpec
from learnlake.normalization.resolver import EventTypeResolver
from learnlake.normalization.values import cast_value, get_path
from learnlake.plugins import TransformRegistry


class MappingError(ValueError):
    pass


class MappingEvaluator:
    def __init__(
        self,
        spec: MappingSpec,
        *,
        resolvers: dict[str, EventTypeResolver] | None = None,
        registry: TransformRegistry | None = None,
    ) -> None:
        self.spec = spec
        self.resolvers = resolvers or {}
        self.registry = registry or TransformRegistry()

    def evaluate_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return {target: self.evaluate_field(mapping, record) for target, mapping in self.spec.fields.items()}

    def evaluate_field(self, mapping: FieldMapping, record: dict[str, Any]) -> Any:
        if mapping.path is not None:
            value = get_path(record, mapping.path)
        elif mapping.const is not None:
            value = mapping.const
        elif mapping.coalesce is not None:
            value = None
            for candidate in mapping.coalesce:
                value = self.evaluate_field(candidate, record)
                if value is not None:
                    break
        elif mapping.resolver is not None:
            resolver = self.resolvers.get(mapping.resolver.name)
            if resolver is None:
                raise MappingError(f"Unknown resolver: {mapping.resolver.name}")
            input_value = get_path(record, mapping.resolver.input)
            value = resolver.resolve(input_value, mapping.resolver.output)
        elif mapping.plugin is not None:
            transform = self.registry.get(mapping.plugin.name)
            inputs = [self.evaluate_field(FieldMapping.model_validate(item), record) for item in mapping.plugin.inputs]
            value = transform(*inputs)
        else:
            raise MappingError("Unsupported mapping operation")
        return cast_value(value, mapping.cast)
