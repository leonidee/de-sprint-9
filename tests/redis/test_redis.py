import sys

sys.path.append("/app")
from src.redis import RedisClient

def main():
    r = RedisClient()

    menu = r.get(key="a51e4e31ae4602047ec52534")['menu']

    for i in menu:
        if i['_id'] == 'c5b64c8ba71adb08ce4bf7bc':
            print(i['category'])

if __name__ == "__main__":
    main()
