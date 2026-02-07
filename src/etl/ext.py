#!/usr/bin/env python3
from alpha_vantage.timeseries import TimeSeries
import pandas as pd

# API Key
API_KEY = "J0ZKQ51NLRLRAIYC"

# Inicializa la API
ts = TimeSeries(key=API_KEY, output_format='pandas')

# Obtén datos diarios de una acción (ejemplo: Tesla TSLA)
data, meta_data = ts.get_daily(symbol='TSLA', outputsize='compact')

# Muestra las primeras filas
print(data.head())
