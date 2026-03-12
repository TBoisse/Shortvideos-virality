import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Exemple de DataFrame
df = pd.read_csv("data.csv", sep='\t')

# fig, axes = plt.subplots(nrows = 2, ncols = 2, figsize=(20,20))
# axes = axes.flatten()
# columns = ["views", "likes", "comments"]
# for i in range(len(columns)):
#     col = columns[i]
#     print("*-* Doing", col)
    
#     df[col] = df[col].fillna(-1)
#     df[col] = df[col].astype(int)
#     ax = axes[i]
#     bins = np.logspace(0, np.log10(df[col].max()), 50)
#     ax.hist(df[col], bins=bins, edgecolor='black')
#     ax.set_xscale('log')
#     ax.set_xlabel("Nombre de vues (log scale)")
#     ax.set_ylabel("Nombre de vidéos")
#     ax.set_title(f"Histogramme des {col} avec échelle logarithmique")
# plt.show()