"""Allow ``python -m sqldim.cli`` invocation."""
import sys
from sqldim.cli import main

if __name__ == "__main__":
    sys.exit(main())
