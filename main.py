import numpy as np
import pandas as pd

lst01 = [[1, 2, 3, 4], [10, 20, 30, 40]]

ndarr = np.array(lst01)

print(ndarr * 100)
print(type(ndarr))

print(ndarr[:, 2])

lst01 = [1, 2, 3]

s01 = pd.Series(lst01)
print(s01, type(s01))
# dddd