import pandas as pd

def transform_energy(bronze_path: str, cfg: dict) -> pd.DataFrame:
    # lê o parquet da Bronze (ou CSV, se preferir)
    df = pd.read_parquet(bronze_path) if bronze_path.endswith(".parquet") else pd.read_csv(bronze_path)

    # normaliza nomes de coluna
    df.columns = [str(c).strip().lower() for c in df.columns]

    # decide o nome da coluna de tempo a partir do config
    # (fallback para 'timestamp' se não houver no YAML)
    dt_col = (
        cfg.get("sources", {})
           .get("energy", {})
           .get("datetime_col", "timestamp")
    ).lower()

    # mapeia apelidos comuns -> 'timestamp'
    alias = {
        "timestamp": "timestamp",
        "datetime": "timestamp",
        "datahora": "timestamp",
        "time": "timestamp",
        "ts": "timestamp",
    }
    # se houver a coluna esperada no config (ex.: 'timestamp' ou 'datetime'), usa
    if dt_col in df.columns and "timestamp" not in df.columns:
        df = df.rename(columns={dt_col: "timestamp"})
    else:
        # senão tenta os apelidos
        for a, tgt in alias.items():
            if a in df.columns and "timestamp" not in df.columns:
                df = df.rename(columns={a: tgt})
                break

    if "timestamp" not in df.columns:
        # erro amigável com as colunas encontradas
        cols = ", ".join(df.columns)
        raise KeyError(f"Coluna de tempo não encontrada. Procurei por '{dt_col}' e aliases {list(alias.keys())}. Colunas disponíveis: {cols}")

    # colunas mínimas (seleciona as que existirem)
    required = ["timestamp", "site_code", "line_code", "equip_code", "kwh"]
    keep = [c for c in required if c in df.columns] + [c for c in df.columns if c not in required]
    df = df[keep].copy()

    # tipos e derivadas
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["dt"] = df["timestamp"].dt.floor("D")

    if "kwh" in df.columns:
        df["kwh"] = pd.to_numeric(df["kwh"], errors="coerce").fillna(0).clip(lower=0)

    return df