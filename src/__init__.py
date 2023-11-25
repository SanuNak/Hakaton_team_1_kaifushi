import os
import sys

from lib.vertica_conn import VerticaConn
from lib.from_s3_loader import fetch_s3_file


sys.path.append(os.path.dirname(os.path.realpath(__file__)))