import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

filepath = "/Users/chuckschultz/Downloads/2020.csv"
data = pd.read_csv(filepath).head(1)
data.head()