#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen


def normalize_key(key: str) -> str:
    return "".join(ch for ch in key.lower() if ch.isalnum())


FIELD_ALIASES = {
    "round": ["round", "drawno", "drawnum", "draw", "회차"],
    "date": ["date", "drawdate", "추첨일"],
    "num1": ["num1", "n1", "no1", "번호1"],
    "num2": ["num2", "n2", "no2", "번호2"],
    "num3": ["num3", "n3", "no3", "번호3"],
    "num4": ["num4", "n4", "no4", "번호4"],
    "num5": ["num5", "n5", "no5", "번호5"],
    "num6": ["num6", "n6", "no6", "번호6"],
    "bonus": ["bonusnumber", "bonus", "보너스", "보너스번호"],
}


def pick_value(row: dict[str, str], aliases: list[str]) -> str | None:
    for alias in aliases:
        value = row.get(alias)
        if value:
            return value
    return None


def parse_int(value: str, field_name: str, row_num: int) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f"row {row_num}: invalid integer in '{field_name}' -> {value!r}"
        ) from exc


def parse_date(value: str, field_name: str, row_num: int) -> str:
    if not re.match(r"^\d{4}\.\d{2}\.\d{2}$", value):
        raise ValueError(
            f"row {row_num}: invalid date format in '{field_name}' -> {value!r} (expected yyyy.mm.dd)"
        )
    try:
        datetime.strptime(value, "%Y.%m.%d")
        return value
    except ValueError as exc:
        raise ValueError(
            f"row {row_num}: invalid calendar date in '{field_name}' -> {value!r}"
        ) from exc


def get_csv_source_url() -> str:
    custom_url = os.getenv("GOOGLE_SHEET_CSV_URL")
    if custom_url:
        return custom_url

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError(
            "GOOGLE_SHEET_ID is required (or set GOOGLE_SHEET_CSV_URL directly)."
        )

    gid = os.getenv("GOOGLE_SHEET_GID", "0")
    return (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )


def fetch_csv_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": "lotto-data-sync/1.0"})
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8-sig")


def parse_draws(csv_text: str) -> list[dict]:
    reader = csv.DictReader(csv_text.splitlines())
    if not reader.fieldnames:
        raise RuntimeError("CSV header not found.")

    draws: list[dict] = []
    for row_num, raw_row in enumerate(reader, start=2):
        row = {
            normalize_key(key): (value or "").strip()
            for key, value in raw_row.items()
            if key
        }

        round_raw = pick_value(row, FIELD_ALIASES["round"])
        if not round_raw:
            # Empty rows are ignored.
            continue

        draw_date = pick_value(row, FIELD_ALIASES["date"])
        bonus_raw = pick_value(row, FIELD_ALIASES["bonus"])
        n_values = [pick_value(row, FIELD_ALIASES[f"num{i}"]) for i in range(1, 7)]

        missing = []
        if not draw_date:
            missing.append("date")
        if not bonus_raw:
            missing.append("bonus")
        if any(not value for value in n_values):
            missing.append("num1~num6")
        if missing:
            raise RuntimeError(f"row {row_num}: missing fields ({', '.join(missing)})")

        draw = {
            "round": parse_int(round_raw, "round", row_num),
            "num1": parse_int(n_values[0], "num1", row_num),
            "num2": parse_int(n_values[1], "num2", row_num),
            "num3": parse_int(n_values[2], "num3", row_num),
            "num4": parse_int(n_values[3], "num4", row_num),
            "num5": parse_int(n_values[4], "num5", row_num),
            "num6": parse_int(n_values[5], "num6", row_num),
            "bonus": parse_int(bonus_raw, "bonus", row_num),
            "date": parse_date(draw_date, "date", row_num),
        }
        draws.append(draw)

    if not draws:
        raise RuntimeError("No valid draw rows found in CSV.")

    return sorted(draws, key=lambda item: item["round"])


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def write_outputs(draws: list[dict]) -> None:
    write_json(Path("lotto.json"), dict(draws[-1]))


def main() -> None:
    source_url = get_csv_source_url()
    csv_text = fetch_csv_text(source_url)
    draws = parse_draws(csv_text)
    write_outputs(draws)
    print(f"Synced {len(draws)} draw(s) from: {source_url} -> lotto.json")


if __name__ == "__main__":
    main()
