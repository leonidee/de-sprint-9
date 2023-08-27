import uuid


class Main:
    __slots__ = "first"

    def __init__(self, first: str) -> None:
        self.first = first


class Test(Main):
    __slots__ = super().__slots__

    def __init__(self, first: str) -> None:
        super().__init__(first)


def main():
    m = Main(first="some string here")

    print(m.first)
    m.second = "some"
    print(m.second)


if __name__ == "__main__":
    main()
