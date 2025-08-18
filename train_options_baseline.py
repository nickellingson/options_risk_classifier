import os, numpy as np, pandas as pd, pyarrow.parquet as pq
import torch, torch.nn as nn, torch.optim as optim
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, classification_report
import math
from sklearn.metrics import classification_report
import joblib, pathlib

PARQUET_DIR = "stage/pytorch_full.parquet"

FEATURES = [
    "option_is_call","strike_price","underlying_close",
    "T_years","moneyness","volume","option_close",
]
TARGET = "label_expire_itm"

# ---- load full parquet folder to pandas
parts = []
for root, _, files in os.walk(PARQUET_DIR):
    for f in files:
        if f.endswith(".parquet"):
            parts.append(pq.read_table(os.path.join(root, f)).to_pandas())
assert parts, f"No parquet files in {PARQUET_DIR}"
df = pd.concat(parts, ignore_index=True)

# basic hygiene
df = df.dropna(subset=FEATURES + [TARGET, "trade_date"]).copy()
df[TARGET] = df[TARGET].astype(int)
df["trade_date"] = pd.to_datetime(df["trade_date"])

# chronological 80/20 split to avoid leakage
dates_sorted = np.sort(df["trade_date"].unique())
cutoff = dates_sorted[int(len(dates_sorted) * 0.8)]
train_df = df[df["trade_date"] <= cutoff]
test_df  = df[df["trade_date"] >  cutoff]

X_train = train_df[FEATURES].to_numpy(np.float32)
y_train = train_df[TARGET].to_numpy(np.float32)
X_test  = test_df[FEATURES].to_numpy(np.float32)
y_test  = test_df[TARGET].to_numpy(np.float32)

# scale (fit on train only)
# z-score normalization... I think, for faster convergance, better performance
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train).astype(np.float32)
X_test  = scaler.transform(X_test).astype(np.float32)

# tensors & loaders
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
Xt, yt = torch.from_numpy(X_train).to(device), torch.from_numpy(y_train).to(device)
Xv, yv = torch.from_numpy(X_test ).to(device), torch.from_numpy(y_test ).to(device)

train_loader = torch.utils.data.DataLoader(torch.utils.data.TensorDataset(Xt, yt), batch_size=2048, shuffle=True)
valid_loader = torch.utils.data.DataLoader(torch.utils.data.TensorDataset(Xv, yv), batch_size=4096, shuffle=False)

# class weighting (helps if 0/1 not 50/50)
pos = (y_train == 1).sum(); neg = (y_train == 0).sum()

pos_weight = torch.tensor([neg / max(pos, 1)], dtype=torch.float32, device=device)

# model
class MLP(nn.Module):
    def __init__(self, d):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, 64), nn.ReLU(),
            nn.Linear(64, 32), nn.ReLU(),
            nn.Linear(32, 1)
        )
    def forward(self, x): return self.net(x).squeeze(-1)

model = MLP(X_train.shape[1]).to(device)
criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
optimizer = optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)

def eval_loader(loader):
    model.eval(); ys=[]; ps=[]
    with torch.no_grad():
        for xb,yb in loader:
            prob = torch.sigmoid(model(xb))
            ys.append(yb.cpu().numpy()); ps.append(prob.cpu().numpy())
    y = np.concatenate(ys); p = np.concatenate(ps)
    auc = roc_auc_score(y, p) if len(np.unique(y)) > 1 else float("nan")
    pred = (p >= 0.5).astype(int)
    return auc, y, p, pred

best_auc, best_state, patience = -1, None, 0
for epoch in range(1, 40+1):
    model.train()
    for xb, yb in train_loader:
        optimizer.zero_grad()
        loss = criterion(model(xb), yb)
        loss.backward(); optimizer.step()
    auc, y, p, pred = eval_loader(valid_loader)
    print(f"Epoch {epoch:02d} AUC={auc:.4f}")
    if auc > best_auc + 1e-4:
        best_auc, best_state, patience = auc, {k:v.cpu() for k,v in model.state_dict().items()}, 0
    else:
        patience += 1
        if patience >= 6:
            print("Early stopping."); break

if best_state: model.load_state_dict({k:v.to(device) for k,v in best_state.items()})
auc, y, p, pred = eval_loader(valid_loader)
print(f"[FINAL] Test AUC={auc:.4f}")

print(classification_report(y, pred, digits=3))

# save
pathlib.Path("models").mkdir(exist_ok=True)
joblib.dump(scaler, "models/scaler_spy_itm.joblib")
torch.save(model.state_dict(), "models/mlp_spy_itm.pt")
print("[done] saved to models/")