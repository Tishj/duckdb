import sys
import duckdb

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "ui":
        duckdb.sql("CALL start_ui()").execute()
        while (True):
            pass
    else:
        print("Usage: python -m duckdb [ui]")

if __name__ == '__main__':
    main()
