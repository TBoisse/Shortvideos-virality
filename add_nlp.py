import nbformat
from nbformat.v4 import new_markdown_cell, new_code_cell
import re

nb_path = 'data_engineering/feature_engineering.ipynb'
with open(nb_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

# 1. Add imports to cell 1 (the one with the imports)
if 'TfidfVectorizer' not in nb.cells[1].source:
    nb.cells[1].source += '\nimport re\nfrom sklearn.feature_extraction.text import TfidfVectorizer\n'

# 2. Create the new NLP cells
new_cells = [
    new_markdown_cell("## 1.5. Extraction de Caractéristiques (NLP sur la Description)\nOn extrait d'abord des **métriques structurelles** : la longueur, les hashtags, les liens..."),
    new_code_cell('''# Fonctions de calcul NLP
def count_links(text):
    return len(re.findall(r'http[s]?://', str(text)))

def count_hashtags(text):
    return len(re.findall(r'#(\w+)', str(text)))

def shouting_ratio(text):
    text_str = str(text)
    if len(text_str) == 0: return 0
    return sum(1 for c in text_str if c.isupper()) / len(text_str)

df['desc_length'] = df['description'].fillna('').apply(lambda x: len(str(x)))
df['desc_num_hashtags'] = df['description'].fillna('').apply(count_hashtags)
df['desc_has_links'] = df['description'].fillna('').apply(count_links)
df['desc_shouting_ratio'] = df['description'].fillna('').apply(shouting_ratio)

print("Métriques structurelles ajoutées !")
df[['description', 'desc_length', 'desc_num_hashtags', 'desc_has_links']].head(3)'''),

    new_markdown_cell('Ensuite, on utilise **TF-IDF** pour trouver les 50 mots les plus discriminants (mots-clés).'),
    new_code_cell('''# On remplace les NaN par du texte vide ou notre token
texts = df['description'].fillna('')

from sklearn.feature_extraction.text import TfidfVectorizer
tfidf = TfidfVectorizer(max_features=50, stop_words='english')
tfidf_matrix = tfidf.fit_transform(texts)
tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=[f"word_{w}" for w in tfidf.get_feature_names_out()])

# On ajoute ces 50 colonnes au DataFrame principal
df = pd.concat([df, tfidf_df], axis=1)

print(f"{tfidf_df.shape[1]} Mots-clés extraits via TF-IDF !")
print("Exemple de mots :", list(tfidf_df.columns)[:10])''')
]

# Insert after cell 3
for i, cell in reversed(list(enumerate(new_cells))):
    nb.cells.insert(4, cell)

with open(nb_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print('NLP cells successfully inserted into notebook.')
