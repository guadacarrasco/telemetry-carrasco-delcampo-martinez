import os
import sys

# Expose shared layer modules to all test files
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas", "layer", "python"))
