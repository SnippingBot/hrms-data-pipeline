"""Part C analysis: gender pay gap and salary anomaly flags.

Reads the loaded Delta tables, converts salaries to USD using data/fx_rates.csv,
answers both Part C questions, and writes:
    output/part_c_report.md
    output/flagged_salaries.csv
"""
import os
import sys

import numpy as np
import pandas as pd

# Thresholds in one place so they are easy to explain and change
MIN_PEER_GROUP = 8        # smallest peer group we trust for outlier detection
ROBUST_Z_CUTOFF = 3.5     # standard cutoff for the modified z score
HARD_MIN_USD = 1_000      # below this a yearly salary is almost certainly a data error
HARD_MAX_USD = 1_000_000  # above this a yearly salary needs a human look
N_BOOT = 1000


# ---------------------------------------------------------------- loading
def load_tables():
    os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join("C:\\hadoop", "bin")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("HRMS_Analytics")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    emp = spark.read.format("delta").load("output/employee").toPandas()
    sal = spark.read.format("delta").load("output/salary").toPandas()
    return emp.merge(sal, on="source_employee_id", how="inner")


def load_fx(path="data/fx_rates.csv"):
    fx = pd.read_csv(path)
    print("fx_rates.csv columns:", list(fx.columns), flush=True)
    lower = {c.lower().strip(): c for c in fx.columns}

    # Currency column: a name match first, otherwise the first text column
    cur_col = next((c for k, c in lower.items() if "curr" in k or "ccy" in k or k in ("code", "iso")), None)
    if cur_col is None:
        cur_col = next(c for c in fx.columns if not pd.api.types.is_numeric_dtype(fx[c]))

    # Rate column: prefer a name hint, otherwise the first numeric column that is not a date or year
    numeric = [c for c in fx.columns if c != cur_col and pd.api.types.is_numeric_dtype(fx[c])]
    hints = ("rate", "usd", "fx", "to_", "per", "value", "conversion", "factor")
    rate_col = next((c for c in numeric if any(h in c.lower() for h in hints)), None)
    if rate_col is None and numeric:
        rate_col = numeric[0]
    if rate_col is None:
        raise ValueError(f"No numeric rate column found in {path}. Columns: {list(fx.columns)}")
    print(f"Using currency column '{cur_col}' and rate column '{rate_col}'", flush=True)

    fx = fx[[cur_col, rate_col]].copy()
    fx.columns = ["currency", "rate"]
    fx["currency"] = fx["currency"].astype(str).str.strip().str.upper()
    fx["rate"] = pd.to_numeric(fx["rate"], errors="coerce")
    fx = fx.dropna().drop_duplicates("currency", keep="last")
    return dict(zip(fx["currency"], fx["rate"]))


def to_usd(df, rates):
    """Rates may be quoted as local per USD or USD per local. Detect which."""
    non_usd = [v for k, v in rates.items() if k != "USD"]
    if "INR" in rates:
        # One rupee is worth far less than one dollar, so INR above 1 means local per USD
        local_per_usd = rates["INR"] > 1
    else:
        local_per_usd = bool(non_usd) and float(np.median(non_usd)) > 1
    rates = dict(rates, USD=1.0)
    r = df["currency"].astype(str).str.upper().map(rates)
    usd = df["annual_base_local"].astype(float) / r if local_per_usd else df["annual_base_local"].astype(float) * r
    out = df.copy()
    out["annual_base_usd"] = usd
    out.attrs["fx_direction"] = "local per USD" if local_per_usd else "USD per local"
    return out


# ------------------------------------------------------------ gender gap
def _design(d, control_cols=("grade", "department", "currency")):
    controls = pd.get_dummies(d[list(control_cols)].astype(str), drop_first=True).astype(float)
    X = controls.copy()
    X.insert(0, "female", (d["gender"] == "F").astype(float))
    X.insert(0, "const", 1.0)
    return X.values


def _female_coef(X, y):
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    return beta[1]


def gender_gap(d, n_boot=N_BOOT, seed=7):
    d = d.copy()
    d["gender"] = d["gender"].astype(str).str.upper().str[0]
    d = d[d["gender"].isin(["M", "F"]) & (d["annual_base_usd"] > 0)].dropna(subset=["grade", "department", "currency"])
    f = d[d["gender"] == "F"]["annual_base_usd"]
    m = d[d["gender"] == "M"]["annual_base_usd"]

    raw_mean_gap = f.mean() / m.mean() - 1
    raw_median_gap = f.median() / m.median() - 1

    X = _design(d)
    y = np.log(d["annual_base_usd"].values)
    adj = np.exp(_female_coef(X, y)) - 1

    rng = np.random.default_rng(seed)
    n = len(d)
    boots = []
    for _ in range(n_boot):
        idx = rng.integers(0, n, n)
        boots.append(np.exp(_female_coef(X[idx], y[idx])) - 1)
    lo, hi = np.percentile(boots, [2.5, 97.5])

    return {
        "n_f": len(f), "n_m": len(m),
        "raw_mean_gap": raw_mean_gap, "raw_median_gap": raw_median_gap,
        "adj_gap": adj, "ci_lo": lo, "ci_hi": hi,
    }


def confidence_label(g):
    width = (g["ci_hi"] - g["ci_lo"]) * 100
    excludes_zero = g["ci_lo"] > 0 or g["ci_hi"] < 0
    if not excludes_zero:
        return "Low, the interval includes zero, so the data does not show a reliable gap in either direction"
    if width <= 4 and min(g["n_f"], g["n_m"]) >= 200:
        return "High, the interval excludes zero and is narrow"
    return "Moderate, the interval excludes zero but is wide or the groups are small"


# ------------------------------------------------------------ anomalies
def flag_salaries(d):
    d = d.copy()
    d = d[d["annual_base_usd"].notna()].copy()
    d["log_usd"] = np.log(d["annual_base_usd"].clip(lower=1))

    d["peer_group"] = d["grade"].astype(str) + " | " + d["currency"].astype(str)
    sizes = d.groupby("peer_group")["log_usd"].transform("size")
    d.loc[sizes < MIN_PEER_GROUP, "peer_group"] = d["grade"].astype(str) + " | all"

    grp = d.groupby("peer_group")["log_usd"]
    med = grp.transform("median")
    mad = (d["log_usd"] - med).abs().groupby(d["peer_group"]).transform("median")
    d["robust_z"] = np.where(mad > 0, 0.6745 * (d["log_usd"] - med) / mad, np.nan)
    d["peer_median_usd"] = np.exp(med)

    reasons = []
    for _, r in d.iterrows():
        why = []
        if r["annual_base_usd"] < HARD_MIN_USD:
            why.append(f"below {HARD_MIN_USD} USD a year, likely a data error")
        if r["annual_base_usd"] > HARD_MAX_USD:
            why.append(f"above {HARD_MAX_USD} USD a year, needs review")
        z = r["robust_z"]
        if pd.notna(z) and z > ROBUST_Z_CUTOFF:
            why.append("far above peers in the same grade and currency")
        if pd.notna(z) and z < -ROBUST_Z_CUTOFF:
            monthly_z = 0.6745 * (np.log(r["annual_base_usd"] * 12) - np.log(r["peer_median_usd"])) / 1.0
            hint = ", may be a monthly figure" if abs(monthly_z) < 1 else ""
            why.append("far below peers in the same grade and currency" + hint)
        reasons.append("; ".join(why))
    d["flag_reason"] = reasons
    return d[d["flag_reason"] != ""].sort_values("robust_z", key=lambda s: s.abs(), ascending=False)


# ------------------------------------------------------ supporting evidence
def _clean_gender(d):
    d = d.copy()
    d["gender"] = d["gender"].astype(str).str.upper().str[0]
    return d


def data_profile(df):
    d = _clean_gender(df)
    counts = d["gender"].value_counts()
    return {
        "rows": len(d),
        "men": int(counts.get("M", 0)),
        "women": int(counts.get("F", 0)),
        "other_gender": int(len(d) - counts.get("M", 0) - counts.get("F", 0)),
        "currencies": d["currency"].value_counts().to_dict(),
        "grades": int(d["grade"].nunique()),
        "departments": int(d["department"].nunique()),
        "missing_department": int(d["department"].isna().sum()),
    }


def gap_by_group(df, col, min_each=5):
    """Median pay of women versus men inside each group, in USD, with headcounts."""
    d = _clean_gender(df)
    d = d[d["gender"].isin(["M", "F"]) & (d["annual_base_usd"] > 0)]
    rows = []
    for key, grp in d.groupby(col):
        f = grp[grp["gender"] == "F"]["annual_base_usd"]
        m = grp[grp["gender"] == "M"]["annual_base_usd"]
        if len(f) >= min_each and len(m) >= min_each:
            rows.append((str(key), f.median() / m.median() - 1, len(f), len(m)))
    return sorted(rows, key=lambda r: r[1])


def sensitivity(df):
    """Point estimate of the adjusted gap under alternative model choices."""
    d = _clean_gender(df)
    d = d[d["gender"].isin(["M", "F"]) & (d["annual_base_usd"] > 0)].dropna(subset=["grade", "department", "currency"])
    results = {}

    def fit(sub, cols):
        X = _design(sub, cols)
        y = np.log(sub["annual_base_usd"].values)
        return np.exp(_female_coef(X, y)) - 1

    results["controlling for grade and currency only"] = fit(d, ("grade", "currency"))
    results["controlling for grade, department and currency"] = fit(d, ("grade", "department", "currency"))
    if "employment_status" in d.columns:
        active = d[d["employment_status"] == "Active"]
        if len(active) > 50 and active["gender"].nunique() == 2:
            results["using active employees only"] = fit(active, ("grade", "department", "currency"))
    trimmed = d[(d["annual_base_usd"] >= d["annual_base_usd"].quantile(0.01)) &
                (d["annual_base_usd"] <= d["annual_base_usd"].quantile(0.99))]
    results["after dropping the top and bottom 1 percent of salaries"] = fit(trimmed, ("grade", "department", "currency"))
    return results


def flag_summary(flagged, df):
    d = _clean_gender(df)
    out = {"total": len(flagged)}
    reasons = flagged["flag_reason"]
    out["high"] = int(reasons.str.contains("far above").sum())
    out["low"] = int(reasons.str.contains("far below").sum())
    out["monthly"] = int(reasons.str.contains("monthly").sum())
    out["hard_low"] = int(reasons.str.contains("below 1000").sum())
    out["hard_high"] = int(reasons.str.contains("above 1000000").sum())
    by_dept = flagged["department"].value_counts().head(5).to_dict()
    out["top_departments"] = by_dept
    fg = _clean_gender(flagged)
    if len(fg):
        share_f_flagged = (fg["gender"] == "F").mean()
        share_f_all = (d["gender"] == "F").mean()
        out["share_f_flagged"] = share_f_flagged
        out["share_f_all"] = share_f_all
    return out


# --------------------------------------------------------------- report
def pct(x):
    return f"{x * 100:+.1f}%"


def less_more(x):
    """Plain words for a gap, for example 5.2% less or 3.0% more."""
    return f"{abs(x) * 100:.1f}% {'less' if x < 0 else 'more'}"


def _lines(rows, fmt, limit=None):
    rows = rows if limit is None else rows[:limit]
    return "\n".join(fmt(r) for r in rows)


def build_report(g, flagged, total, fx_direction, profile, by_grade, by_dept, sens, fs):
    if g["ci_hi"] < 0:
        verdict = "Yes. On a like for like basis women are paid less than men."
    elif g["ci_lo"] > 0:
        verdict = "No. On a like for like basis women are paid more than men."
    else:
        verdict = "The data does not show a reliable difference in either direction."

    grade_text = _lines(by_grade, lambda r: f"Grade {r[0]}: women earn {less_more(r[1])} than men at the median ({r[2]} women, {r[3]} men).") or "No grade had enough women and men to compare."
    dept_text = _lines(by_dept, lambda r: f"{r[0]}: women earn {less_more(r[1])} than men at the median ({r[2]} women, {r[3]} men).", limit=8) or "No department had enough women and men to compare."
    sens_text = "\n".join(f"Women earn {less_more(v)} than men, {k}." for k, v in sens.items())
    currencies = ", ".join(f"{k} {v}" for k, v in profile["currencies"].items())

    flag_dept = ", ".join(f"{k} ({v})" for k, v in fs["top_departments"].items()) or "none"
    balance = ""
    if "share_f_flagged" in fs:
        share_all, share_flag = fs["share_f_all"] * 100, fs["share_f_flagged"] * 100
        balance = (f"Women make up {share_all:.0f}% of employees and {share_flag:.0f}% of flagged employees. ")
        if fs["total"] >= 30 and abs(share_all - share_flag) < 10:
            balance += "The two shares are close, so the method does not appear to single out one gender."
        elif fs["total"] < 30:
            balance += "With so few flags this share is not a reliable indicator either way."
        else:
            balance += "The gap between the shares is worth a look before anyone acts on the flags."

    return f"""# Part C Analysis

## 1. Are women paid less than men?

**Answer:** {verdict} Comparing women and men in the same grade, department and pay currency, women earn {less_more(g['adj_gap'])} (95% confidence interval {pct(g['ci_lo'])} to {pct(g['ci_hi'])}).

**Confidence:** {confidence_label(g)}.

The raw gap, which ignores grade and department, is {pct(g['raw_mean_gap'])} on average and {pct(g['raw_median_gap'])} at the median. The raw figure also reflects where women and men sit in the organisation, so the adjusted figure is the fairer answer to the question the HR head asked. A large difference between the two would mean much of the gap comes from who holds senior or higher paying roles and not from pay within a role.

**How the number was produced.** Salaries were converted to USD with fx_rates.csv (rates read as {fx_direction}). I then fitted a regression of log salary on a female indicator plus grade, department and currency, and converted the female coefficient to a percentage. The confidence interval comes from 1000 bootstrap resamples of employees. The comparison uses {g['n_f']} women and {g['n_m']} men from {profile['rows']} loaded employees. {profile['other_gender']} employees with another or missing gender value were left out.

**Where the gap sits.** Median pay of women against men inside each grade:
{grade_text}

Departments, from the largest gap to the smallest, each with at least five women and five men:
{dept_text}

**Does the result depend on modelling choices?**
{sens_text}
A result that stays close to the headline figure across these variations is more trustworthy. If any variation moves it a lot, quote the range and not a single number.

**Limits.** This is observational data, so it shows association and not cause. Grade can itself carry past bias, so a small gap within grade does not rule out unequal access to senior grades. Pay components such as bonus and allowances are not in the loaded salary field, and the salary column for HRMS C was inferred and not confirmed by the customer. Currency and department are used as controls, but location, tenure and performance are not.

## 2. Salaries that look wrong

**Method.** Each employee is compared with peers in the same grade and pay currency, or the same grade alone when a group has fewer than {MIN_PEER_GROUP} people. For each peer group I compute the median and the median absolute deviation of log salary, then a robust score for each person. Anyone beyond {ROBUST_Z_CUTOFF} in either direction is flagged. I chose this over an average and standard deviation rule because extreme values inflate the average and the spread, which lets the very errors we want to find hide themselves. Working on log salary respects the fact that pay differences are proportional.

Two fixed checks run alongside: yearly pay below {HARD_MIN_USD} USD or above {HARD_MAX_USD} USD is flagged regardless of peers. A very low salary that becomes typical when multiplied by 12 is labelled as a possible monthly figure entered in an annual field.

**Result.** {fs['total']} of {total} employees were flagged: {fs['high']} well above peers, {fs['low']} well below peers, of which {fs['monthly']} look like monthly figures. {fs['hard_low']} are under the fixed floor and {fs['hard_high']} are above the fixed ceiling. Departments with the most flags: {flag_dept}. {balance}

The full list with the peer median, the score and the reason for each person is in output/flagged_salaries.csv. These are items to confirm with the customer and not confirmed errors, because a real specialist can legitimately sit far from peers.

## 3. Data used

The analysis covers {profile['rows']} employees ({profile['men']} men, {profile['women']} women), across {profile['grades']} grades and {profile['departments']} departments, with currencies split as {currencies}. {profile['missing_department']} employees have no department and are excluded from the adjusted comparison.
"""


def main():
    df = load_tables()
    rates = load_fx()
    df = to_usd(df, rates)
    fx_direction = df.attrs.get("fx_direction", "unknown")

    g = gender_gap(df)
    flagged = flag_salaries(df)
    profile = data_profile(df)
    by_grade = gap_by_group(df, "grade")
    by_dept = gap_by_group(df, "department")
    sens = sensitivity(df)
    fs = flag_summary(flagged, df)

    os.makedirs("output", exist_ok=True)
    cols = ["source_employee_id", "full_name", "grade", "department", "currency",
            "annual_base_local", "annual_base_usd", "peer_median_usd", "robust_z", "flag_reason"]
    flagged[[c for c in cols if c in flagged.columns]].to_csv("output/flagged_salaries.csv", index=False)

    report = build_report(g, flagged, len(df), fx_direction, profile, by_grade, by_dept, sens, fs)
    with open("output/part_c_report.md", "w", encoding="utf-8") as f:
        f.write(report)
    print(report)


if __name__ == "__main__":
    main()