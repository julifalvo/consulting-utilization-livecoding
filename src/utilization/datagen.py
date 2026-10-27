"""Deterministic synthetic data for the exercise.

Simulates what a consulting firm's data platform receives every Monday:

* ``timesheets/timesheets_<export_date>.json`` - newline-delimited JSON exported by the
  time-tracking SaaS. Each export holds the entries created or changed since the previous one.
* ``reference/*.csv`` - full snapshots from HR (employee history), finance (engagements and
  rate cards) and treasury (FX rates).

The defects below are planted on purpose; the live-coding exercise is about finding and
handling them. They are listed in docs/solution-walkthrough.md, not in INTERVIEW.md.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PERIOD_START = date(2026, 8, 31)  # Monday
PERIOD_WEEKS = 4
EXPORT_DATES = [PERIOD_START + timedelta(weeks=w + 1) for w in range(PERIOD_WEEKS)]

OFFICES = {
    # office: (UTC offset in September, uses comma decimals in its local UI)
    "London": (1, False),
    "New York": (-4, False),
    "Madrid": (2, True),
    "Buenos Aires": (-3, True),
}
LEVELS = ["Analyst", "Consultant", "Manager", "Principal"]
BASE_RATE_USD = {"Analyst": 150, "Consultant": 220, "Manager": 300, "Principal": 420}
CURRENCY_FACTOR = {"USD": 1.0, "GBP": 0.79, "EUR": 0.92}

ENGAGEMENTS = [
    # code, client, billing_type, currency
    ("ENG-001", "Northwind Bank", "TM", "USD"),
    ("ENG-002", "Halcyon Retail", "TM", "GBP"),
    ("ENG-003", "Brightline Energy", "FIXED", "EUR"),
    ("ENG-004", "Meridian Insurance", "TM", "USD"),
    ("ENG-005", "Solace Telecom", "TM", "EUR"),
    ("ENG-006", "Verdant Pharma", "FIXED", "GBP"),
    ("ENG-007", "Ironclad Logistics", "TM", "USD"),
    ("ENG-008", "Lumen Media", "TM", "GBP"),
    ("INT-001", "Internal - Training", "INTERNAL", "USD"),
    ("INT-002", "Internal - Business Development", "INTERNAL", "USD"),
]
CLIENT_CODES = [code for code, _, kind, _ in ENGAGEMENTS if kind != "INTERNAL"]

FIRST_NAMES = [
    "Ava", "Liam", "Sofia", "Noah", "Mia", "Lucas", "Emma", "Mateo", "Olivia", "Hugo",
    "Chloe", "Leo", "Isla", "Diego", "Grace", "Martin", "Zoe", "Tomas", "Ella", "Ivan",
    "Lucia", "Oscar", "Nora", "Bruno", "Maya", "Felix", "Alma", "Theo", "Vera", "Iker",
    "Ruby", "Dante",
]  # fmt: skip
LAST_NAMES = [
    "Walker", "Garcia", "Chen", "Rossi", "Okafor", "Silva", "Novak", "Haddad", "Moreau",
    "Kowalski", "Fischer", "Tanaka", "Lopez", "Murphy", "Ibarra", "Sato",
]  # fmt: skip


@dataclass
class Employee:
    employee_id: str
    full_name: str
    office: str
    level: str
    hired: date = PERIOD_START - timedelta(days=400)
    left: date | None = None  # exclusive
    promotion: tuple[date, str] | None = None
    engagements: list[str] = field(default_factory=list)


def _employees(rng: random.Random) -> list[Employee]:
    offices = list(OFFICES)
    people = []
    for i in range(32):
        emp_id = f"E{i + 1:03d}"
        level = LEVELS[min(3, int(rng.random() ** 1.6 * 4))]
        people.append(
            Employee(
                employee_id=emp_id,
                full_name=f"{FIRST_NAMES[i]} {LAST_NAMES[i % len(LAST_NAMES)]}",
                office=offices[i % len(offices)],
                level=level,
                engagements=rng.sample(CLIENT_CODES, 2),
            )
        )
    by_id = {e.employee_id: e for e in people}
    # Mid-period HR events: they make the employee dimension a real SCD2 history.
    by_id["E003"].level, by_id["E003"].promotion = "Consultant", (date(2026, 9, 14), "Manager")
    by_id["E010"].level, by_id["E010"].promotion = "Analyst", (date(2026, 9, 14), "Consultant")
    by_id["E021"].level, by_id["E021"].promotion = "Manager", (date(2026, 9, 21), "Principal")
    by_id["E021"].engagements = ["ENG-005", "ENG-001"]  # ENG-005 has no Principal rate card
    by_id["E031"].hired = date(2026, 9, 16)  # joins on a Wednesday
    by_id["E032"].left = date(2026, 9, 12)  # last working day Friday 2026-09-11
    by_id["E003"].office = "Madrid"  # promoted employee with comma decimals + CEST offsets
    # Bench: E028 lives on internal work, E029 stops logging in week 3.
    by_id["E028"].engagements = ["INT-001", "INT-002"]
    return people


def _level_on(emp: Employee, day: date) -> str:
    if emp.promotion and day >= emp.promotion[0]:
        return emp.promotion[1]
    return emp.level


def _employee_history(people: list[Employee]) -> list[dict]:
    rows = []
    for e in people:
        email = f"{e.full_name.lower().replace(' ', '.')}@example-consulting.com"
        base = {"employee_id": e.employee_id, "full_name": e.full_name, "email": email}
        base["office"] = e.office
        if e.promotion:
            changed, new_level = e.promotion
            rows.append({**base, "level": e.level, "valid_from": e.hired, "valid_to": changed})
            rows.append({**base, "level": new_level, "valid_from": changed, "valid_to": e.left})
        else:
            rows.append({**base, "level": e.level, "valid_from": e.hired, "valid_to": e.left})
    return rows


def _rate_cards() -> list[dict]:
    rows = []
    for idx, (code, _, kind, currency) in enumerate(ENGAGEMENTS):
        if kind == "INTERNAL":
            continue
        for level in LEVELS:
            if code == "ENG-005" and level == "Principal":
                continue  # planted gap: discovered once E021 is promoted
            premium = 1 + (idx % 3) * 0.05
            rate = BASE_RATE_USD[level] * CURRENCY_FACTOR[currency] * premium
            rows.append(
                {
                    "engagement_code": code,
                    "level": level,
                    "bill_rate": round(rate, 2),
                    "currency": currency,
                }
            )
    return rows


def _fx_rates(rng: random.Random) -> list[dict]:
    rows = []
    level = {"GBP": 1.27, "EUR": 1.09}
    day = PERIOD_START - timedelta(days=7)
    while day <= PERIOD_START + timedelta(weeks=PERIOD_WEEKS):
        for currency in ("GBP", "EUR"):
            level[currency] *= 1 + rng.uniform(-0.004, 0.004)
            is_weekend = day.weekday() >= 5
            uk_bank_holiday = currency == "GBP" and day == date(2026, 8, 31)
            if not is_weekend and not uk_bank_holiday:
                rows.append(
                    {
                        "rate_date": day,
                        "currency": currency,
                        "usd_per_unit": round(level[currency], 6),
                    }
                )
        day += timedelta(days=1)
    return rows  # no USD rows: USD -> USD is implicitly 1.0


def _ts(day: date, hour: float, office: str) -> str:
    """Local wall-clock timestamp rendered with the office's UTC offset."""
    offset = OFFICES[office][0]
    local = datetime(day.year, day.month, day.day, tzinfo=timezone(timedelta(hours=offset)))
    return (local + timedelta(hours=hour)).isoformat(timespec="seconds")


def _hours_value(hours: float, office: str, rng: random.Random) -> float | str:
    if OFFICES[office][1] and rng.random() < 0.35:
        return f"{hours:g}".replace(".", ",")  # "7,5" typed into a localized UI
    return hours


def _entries(people: list[Employee], rng: random.Random) -> list[dict]:
    """Original timesheet entries, before any correction or export artifact."""
    entries: list[dict] = []
    seq = 0
    for week in range(PERIOD_WEEKS):
        for weekday in range(5):
            day = PERIOD_START + timedelta(weeks=week, days=weekday)
            for e in people:
                if day < e.hired or (e.left and day >= e.left):
                    continue
                if e.employee_id == "E029" and week >= 2:
                    continue  # rolled off and not logging: on the bench
                if rng.random() < 0.04:
                    continue  # forgot to log this day
                primary, secondary = e.engagements
                split = rng.random()
                if split < 0.65:
                    blocks = [(primary, rng.choice([7.5, 8, 8, 8.5, 9]))]
                elif split < 0.85:
                    blocks = [(primary, rng.choice([4, 5, 6])), (secondary, rng.choice([2, 3, 4]))]
                else:
                    blocks = [(primary, rng.choice([5, 6, 7])), ("INT-001", rng.choice([1, 2]))]
                for engagement, hours in blocks:
                    seq += 1
                    entries.append(
                        {
                            "entry_id": f"TE-{seq:06d}",
                            "employee_id": e.employee_id,
                            "engagement_code": engagement,
                            "work_date": day.isoformat(),
                            "hours": _hours_value(hours, e.office, rng),
                            "status": "REJECTED" if rng.random() < 0.02 else "APPROVED",
                            "updated_at": _ts(day, 18 + rng.random() * 3, e.office),
                            "_office": e.office,
                            "_week": week,
                        }
                    )
    return entries


def _export_rows(people: list[Employee], rng: random.Random) -> dict[date, list[dict]]:
    by_id = {e.employee_id: e for e in people}
    originals = _entries(people, rng)
    exports: dict[date, list[dict]] = {d: [] for d in EXPORT_DATES}
    for row in originals:
        exports[EXPORT_DATES[row["_week"]]].append(row)

    for week, export_date in enumerate(EXPORT_DATES):
        rows = exports[export_date]
        if week > 0:
            previous = [r for r in exports[EXPORT_DATES[week - 1]] if r.get("_kind") is None]
            # 1. Overlapping export window: some rows are re-sent unchanged.
            rows += [{**r, "_kind": "resend"} for r in rng.sample(previous, 12)]
            # 2. Late corrections: approved later with different hours, a week after.
            for r in rng.sample(previous, 15):
                work_day = date.fromisoformat(r["work_date"])
                fixed = max(1.0, float(str(r["hours"]).replace(",", ".")) - 1)
                rows.append(
                    {
                        **r,
                        "hours": fixed,
                        "status": "APPROVED",
                        "updated_at": _ts(work_day + timedelta(days=7), 10, r["_office"]),
                        "_kind": "correction",
                    }
                )
        # 3. Invalid values typed by humans.
        for r, bad_hours in zip(rng.sample(rows, 3), [-4, 25, 0], strict=True):
            r["hours"] = bad_hours
        exports[export_date] = rows

    # 4. Timezone trap (export 2): a Madrid entry and its same-evening correction made by
    #    central ops in UTC. Lexicographic order of the raw strings picks the WRONG version.
    trap_emp = by_id["E003"]
    exports[EXPORT_DATES[1]] += [
        {
            "entry_id": "TE-900001",
            "employee_id": trap_emp.employee_id,
            "engagement_code": trap_emp.engagements[0],
            "work_date": "2026-09-10",
            "hours": "9,5",
            "status": "APPROVED",
            "updated_at": "2026-09-11T01:00:00+02:00",  # 2026-09-10 23:00 UTC
        },
        {
            "entry_id": "TE-900001",
            "employee_id": trap_emp.employee_id,
            "engagement_code": trap_emp.engagements[0],
            "work_date": "2026-09-10",
            "hours": 7.5,
            "status": "APPROVED",
            "updated_at": "2026-09-10T23:30:00Z",  # true latest version
        },
    ]
    # 5. Unknown engagement (typo) and an employee HR has never heard of (a contractor).
    exports[EXPORT_DATES[1]].append(
        {
            "entry_id": "TE-900002",
            "employee_id": "E007",
            "engagement_code": "ENG-0O7",
            "work_date": "2026-09-09",
            "hours": 8,
            "status": "APPROVED",
            "updated_at": "2026-09-09T19:00:00-04:00",
        }
    )
    exports[EXPORT_DATES[2]].append(
        {
            "entry_id": "TE-900003",
            "employee_id": "C900",
            "engagement_code": "ENG-004",
            "work_date": "2026-09-16",
            "hours": 8,
            "status": "APPROVED",
            "updated_at": "2026-09-16T19:00:00-04:00",
        }
    )
    # 6. Schema drift: from export 3 the vendor sends soft deletes as tombstones.
    corrected = {r["entry_id"] for r in exports[EXPORT_DATES[2]] if r.get("_kind") == "correction"}
    candidates = [
        r
        for r in exports[EXPORT_DATES[1]]
        if r.get("_week") == 1 and r["entry_id"] not in corrected
    ]
    for r in rng.sample(candidates, 3):
        exports[EXPORT_DATES[2]].append(
            {**r, "is_deleted": True, "updated_at": "2026-09-15T08:00:00Z", "_kind": "tombstone"}
        )
    # 7. Schema drift: from export 4 the vendor adds a new optional column.
    for r in exports[EXPORT_DATES[3]]:
        r["activity_type"] = rng.choice(["delivery", "delivery", "delivery", "travel", "admin"])
    return exports


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})


def generate(out_dir: str | Path, seed: int = 42) -> dict[str, int]:
    """Write the raw landing zone under ``out_dir``. Same seed, same bytes."""
    rng = random.Random(seed)
    out = Path(out_dir)
    people = _employees(rng)

    _write_csv(out / "reference" / "employees.csv", _employee_history(people))
    _write_csv(
        out / "reference" / "engagements.csv",
        [
            {"engagement_code": c, "client_name": n, "billing_type": k, "currency": cur}
            for c, n, k, cur in ENGAGEMENTS
        ],
    )
    _write_csv(out / "reference" / "rate_cards.csv", _rate_cards())
    _write_csv(out / "reference" / "fx_rates.csv", _fx_rates(rng))

    counts = {}
    ts_dir = out / "timesheets"
    ts_dir.mkdir(parents=True, exist_ok=True)
    for export_date, rows in _export_rows(people, rng).items():
        rng.shuffle(rows)
        lines = [
            json.dumps({k: v for k, v in r.items() if not k.startswith("_")}, separators=(",", ":"))
            for r in rows
        ]
        if export_date == EXPORT_DATES[2]:
            lines.insert(len(lines) // 2, lines[0][: len(lines[0]) // 2])  # 8. truncated line
        path = ts_dir / f"timesheets_{export_date.isoformat()}.json"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        counts[path.name] = len(lines)
    return counts
