import pandas as pd
from pathlib import Path


current_file_path = Path(__file__).resolve()
parent_path = current_file_path.parents[2]
df = pd.read_csv(parent_path/'data/raw/gfm_floods.csv')
print(df['severity'].value_counts() if 'severity' in df.columns else "No severity column")