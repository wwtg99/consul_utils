import os
import sys


sys.path.insert(0, os.path.abspath('lib'))
from consul_utils.application import cli


if __name__ == '__main__':
    cli()
