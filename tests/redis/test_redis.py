import sys

sys.path.append("/Users/leonidgrisenkov/Code/de-sprint-9")
from src.redis import RedisClient


def main():
    r = RedisClient()

    print(r.get(key="test"))


if __name__ == "__main__":
    main()
