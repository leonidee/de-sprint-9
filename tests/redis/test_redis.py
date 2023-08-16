import sys

sys.path.append("/app")
from src.redis import RedisClient

import dotenv
dotenv.load_dotenv()

def main():
    r = RedisClient()

    print(r.get(key="test"))


if __name__ == "__main__":
    main()
