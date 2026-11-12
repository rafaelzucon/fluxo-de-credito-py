"""Base dos eventos/mensagens trafegados em JSON: campos em camelCase, valores monetários como número."""

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, PlainSerializer
from pydantic.alias_generators import to_camel

type Valor = Annotated[Decimal, PlainSerializer(float, return_type=float, when_used="json")]
"""Decimal internamente (sem erro de arredondamento); número no JSON, como nas versões Java e Node."""


class ModeloJson(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True, validate_by_alias=True, frozen=True)

    def para_json(self) -> str:
        return self.model_dump_json(by_alias=True)
