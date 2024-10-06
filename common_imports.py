import warnings
import os

os.system("pip install requests pyspark pandas pyarrow matplotlib numpy asyncio aiohttp httpx bs4 fastparquet")

import zipfile, requests, random, asyncio, aiohttp, io
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, DecimalType, DoubleType
from pyspark.sql.functions import regexp_replace, to_date, col, trim, substring, when, mean, format_number
from decimal import Decimal, getcontext, ROUND_HALF_UP
import pyarrow as pa
from bs4 import BeautifulSoup
import httpx

getcontext().prec = 30
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.display.float_format = '{:.2f}'.format

layers = ['staging', 'raw', 'trusted', 'refined']
etls = os.listdir(path='etl/')

if not os.path.exists('_data'):
	os.makedirs('_data')
for layer in layers:
	if not os.path.exists(f'_data/{layer}'):
		os.makedirs(f'_data/{layer}')
	for etl in etls:
		if not os.path.exists(f'_data/{layer}/{etl}'):
			os.makedirs(f'_data/{layer}/{etl}')
