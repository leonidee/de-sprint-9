import sys


sys.path.append("/app")
from src.processor import STGMessageProcessor

def main():

    proc = STGMessageProcessor()

    proc.run()

if __name__ == "__main__":
    main()