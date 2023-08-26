import time
from typing import NoReturn

def main() -> NoReturn:

    i = 1
    while True:
        time.sleep(360)

        print(i)
        i += 1

if __name__ == "__main__":
    main()