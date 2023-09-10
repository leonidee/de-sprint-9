from pydantic_core._pydantic_core import ValidationError


def main():
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).parent.parent))

    from pydantic import BaseModel, Field

    class User(BaseModel):
        age: int = Field(frozen=True, gt=1, lt=10)

    u1 = User(age=8)
    try:
        u2 = User(age=15)
    except ValidationError as err:
        print(err)

    data = {"age": 5}

    u3 = User(**data)
    print(u3)

    u3.name = "Hello"


if __name__ == "__main__":
    main()
