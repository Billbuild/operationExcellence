from logger_config import logger
import easy.easyclass as ec
import easy.easyfunc as ef
import easy.constants as const
import easy.easymetrics as em
import easy.json_handle as jh

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

import json
import os
import datetime as dt
import time
from enum import Enum
from pathlib import Path
from typing import Dict, List, Union

from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

if __name__ == '__main__':
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    start_date = dt.date(2025, 8, 22)
    end_date = dt.date(2025, 9, 22)

    customer = ec.Customer('cus011', conn)
    customer.row()

    customer_owe = ec.CustomerOwe(customer, conn)
    customer_owe.period_values((start_date, end_date))

    print(customer.all_ids())
