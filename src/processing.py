from datetime import datetime, timedelta, timezone

import pandas as pd


def _join_day(join_time_str, ref_date_str) -> int:
    """Game-day the member joined, using the 10:00 UTC day flip.

    Chrono join_time is JST ("2026-08-03T20:29:41"). The game day turns over
    at 10:00 UTC (19:00 JST), so the effective game date of a timestamp is its
    UTC date shifted back by 10 hours. A join before the reference period
    (current month start) means the member was present from day 1. Any parse
    failure degrades to day 1.
    """
    if not join_time_str:
        return 1
    try:
        dt = datetime.fromisoformat(str(join_time_str).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=9)))
        game_date = (dt.astimezone(timezone.utc) - timedelta(hours=10)).date()
        ref_date = datetime.strptime(ref_date_str, "%Y-%m-%d").date()
        if game_date < ref_date:
            return 1
        return game_date.day
    except (ValueError, TypeError):
        return 1


def build_dataframe(data: dict, join_map: dict = None, sdate: str = None) -> pd.DataFrame:
    df = pd.json_normalize(data.get("club_friend_history") or [])
    for c in ("friend_viewer_id", "friend_name", "actual_date", "adjusted_interpolated_fan_gain"):
        if c not in df.columns:
            df[c] = pd.NA

    # Entries without an actual_date would otherwise pivot into a bogus "Day <NA>" column,
    # and mixed nulls coerce the column to float ("Day 5.0"). Normalize to day integers first.
    if "actual_date" in df.columns:
        df["actual_date"] = pd.to_numeric(df["actual_date"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["actual_date"]).copy()

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

    # Gray out pre-join days: a member who joined on game-day N has no real data
    # before N. Chrono backfills 0 (or interpolated phantoms) for those days, so
    # blank them and let the sheet's BLANK rule render them gray.
    if join_map and sdate:
        join_days = {str(k): _join_day(v, sdate) for k, v in join_map.items()}
        for pos, vid in enumerate(df["friend_viewer_id"]):
            jd = join_days.get(str(vid), 1)
            if jd > 1:
                for c in day_cols:
                    if (_day_num(c) or float("inf")) < jd:
                        df.at[df.index[pos], c] = pd.NA

    df["AVG/d"] = df[day_cols].mean(axis=1).round(0) if day_cols else 0
    df["Total"] = df[day_cols].sum(axis=1) if day_cols else 0
    df = df[["friend_viewer_id", "friend_name", "AVG/d"] + day_cols + ["Total"]].rename(
        columns={"friend_viewer_id": "Member_ID", "friend_name": "Member_Name"}
    )
    df["Member_ID"] = df["Member_ID"].fillna("").astype(str)
    df["Member_Name"] = df["Member_Name"].fillna("").astype(str)
    for c in df.columns:
        if c not in ("Member_ID", "Member_Name"):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values(["AVG/d", "Member_Name"], ascending=[False, True], na_position="last", kind="mergesort").reset_index(drop=True)
    return df
