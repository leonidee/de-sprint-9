import dataclasses
import json
from datetime import datetime

from pydantic_core._pydantic_core import ValidationError


def main():
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).parent.parent))

    from pydantic import BaseModel, Field

    class NestedField(BaseModel):
        value: str
        arr: list
        dt: datetime

    class User(BaseModel):
        age: int = Field(frozen=True, gt=1, lt=10)
        nested: NestedField

    u = User(
        age=8,
        nested=NestedField(
            value="some value", arr=[1, 2, 3, 4], dt=datetime(1993, 1, 1)
        ),
    )

    u = u.model_dump_json(indent=2)
    print(type(u))
    print(u)


if __name__ == "__main__":
    main()
