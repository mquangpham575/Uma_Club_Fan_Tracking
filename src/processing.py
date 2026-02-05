import pandas as pd


def build_dataframe(data: dict) -> pd.DataFrame:
    df = pd.json_normalize(data.get("club_friend_history") or [])
    for c in ("friend_viewer_id", "friend_name", "actual_date", "adjusted_interpolated_fan_gain"):
        if c not in df.columns:
            df[c] = pd.NA

    df = (
        df.assign(day_col=lambda d: "Day " + d["actual_date"].astype(str))
            .pivot_table(
                index=["friend_viewer_id", "friend_name"],
                columns="day_col",
                values="adjusted_interpolated_fan_gain",
                aggfunc="first"
            )
            .reset_index()
    )
    df.columns.name = None

    def _day_num(x: str):
        if not isinstance(x, str) or not x.startswith("Day "):
            return None
        try:
            return int(x.split(maxsplit=1)[1])
        except Exception:
            return None

    day_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]

    nums = [n for n in map(_day_num, day_cols) if n is not None]
    if nums:
        latest_day = max(nums)
        latest_col = f"Day {latest_day}"
        if latest_col in df.columns:
            df = df[~df[latest_col].isna()].copy()

    day_cols = sorted(day_cols, key=lambda c: (_day_num(c) if _day_num(c) is not None else float("inf")))

    df["AVG/d"] = df[day_cols].mean(axis=1).round(0) if day_cols else 0
    df = df[["friend_viewer_id", "friend_name", "AVG/d"] + day_cols].rename(
        columns={"friend_viewer_id": "Member_ID", "friend_name": "Member_Name"}
    )
    df["Member_ID"] = df["Member_ID"].fillna("").astype(str)
    df["Member_Name"] = df["Member_Name"].fillna("").astype(str)
    for c in df.columns:
        if c not in ("Member_ID", "Member_Name"):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values(["AVG/d", "Member_Name"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    return df
