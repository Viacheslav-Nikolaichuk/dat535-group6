"""
Altair charts for the dashboard, in one consistent style:

  * every zone keeps its colour on every chart (categorical slots 1-5); workday / weekend use slots 6-7
    so they never look like a zone. Both sets pass the colour-blindness checks of the dataviz palette
    validator in light and dark mode. Three light-mode colours are below 3:1 contrast, so every chart
    has a legend or a title naming the series, and a table view next to it;
  * the bill's schemes (use case 3, its own page) reuse slots 6-7 for strømstønad and Norgespris, with
    "no support" as the muted grey reference line; all three pairs pass the same checks in both modes;
  * 2 px lines, 8 px points, hairline grid, one y-axis per chart, tooltips on hover;
  * prices in øre/kWh (NOK x 100).
"""

from typing import Dict, List, Optional, Sequence

import altair as alt
import pandas as pd
import streamlit as st

from config.settings import PRICE_ZONES

ZONE_COLORS_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
ZONE_COLORS_DARK = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181"]
DAY_TYPE_COLORS_LIGHT = ["#008300", "#6250d6"]
DAY_TYPE_COLORS_DARK = ["#008300", "#9085e9"]
MUTED = "#898781"

DAY_TYPE_LABELS = {"workday": "Workday", "weekend_holiday": "Weekend or holiday"}

# One tick per month: with only a few months, the default ticks fall mid-month and repeat the label.
MONTH_TICKS = {"interval": "month", "step": 1}


def _dark() -> bool:
    theme = getattr(getattr(st, "context", None), "theme", None)
    return getattr(theme, "type", None) == "dark"


def zone_colors() -> Dict[str, str]:
    return dict(zip(PRICE_ZONES, ZONE_COLORS_DARK if _dark() else ZONE_COLORS_LIGHT))


def scheme_colors() -> List[str]:
    """No support, strømstønad, Norgespris (config.price_rules.SCHEMES order)."""
    return [MUTED, *(DAY_TYPE_COLORS_DARK if _dark() else DAY_TYPE_COLORS_LIGHT)]


def _scale(domain: Sequence[str], colors: Sequence[str]) -> alt.Scale:
    return alt.Scale(domain=list(domain), range=list(colors))


def _legend() -> alt.Legend:
    return alt.Legend(orient="top", title=None, symbolType="stroke", symbolStrokeWidth=2)


def lines(
    df: pd.DataFrame,
    x: str,
    y: str,
    y_title: str,
    series: Optional[str] = None,
    domain: Sequence[str] = (),
    colors: Sequence[str] = (),
    x_type: str = "T",
    x_title: Optional[str] = None,
    x_format: Optional[str] = None,
    series_labels: Optional[Dict[str, str]] = None,
    single_color: str = ZONE_COLORS_LIGHT[0],
    hour_of_day: bool = False,
    monthly: bool = False,
    height: int = 300,
) -> alt.Chart:
    """A line per series with a crosshair: hovering anywhere shows every series' value at that x.

    hour_of_day: x holds hours 0-23, labelled as clock times (00:00, 03:00, ...).
    monthly: x holds the first day of each month; one tick per month.
    """
    y_fmt = ".1f"
    hover = alt.selection_point(nearest=True, on="pointerover", fields=[x], empty=False, clear="pointerout")
    if hour_of_day:
        clock = "(datum.{v} < 10 ? '0' : '') + datum.{v} + ':00'"
        x_enc = alt.X(f"{x}:Q", title=x_title, scale=alt.Scale(domain=[0, 23], nice=False),
                      axis=alt.Axis(values=list(range(0, 24, 3)), labelExpr=clock.format(v="value")))
        x_tip = alt.Tooltip("clock:N", title=x_title or "Hour")
    else:
        x_axis = alt.Axis(labelOverlap=True, **({"format": x_format} if x_format else {}),
                          **({"tickCount": MONTH_TICKS} if monthly else {}))
        x_enc = alt.X(f"{x}:{x_type}", title=x_title, axis=x_axis)
        x_tip = alt.Tooltip(f"{x}:{x_type}", title=x_title or "Date", **({"format": x_format} if x_format else {}))

    base = alt.Chart(df)
    if series:
        color = alt.Color(f"{series}:N", scale=_scale(domain, colors), legend=_legend())
        if series_labels:
            color = alt.Color(f"{series}_label:N", scale=_scale([series_labels[d] for d in domain], colors),
                              legend=_legend())
            base = alt.Chart(df.assign(**{f"{series}_label": df[series].map(series_labels)}))
        line = base.mark_line(strokeWidth=2, strokeCap="round", strokeJoin="round").encode(
            x=x_enc, y=alt.Y(f"{y}:Q", title=y_title), color=color)
        wide = df.pivot_table(index=x, columns=series, values=y).reset_index()
        names = [d for d in domain if d in wide.columns]
        tooltip = [x_tip] + [
            alt.Tooltip(f"{name}:Q", title=(series_labels or {}).get(name, name), format=y_fmt) for name in names
        ]
    else:
        line = base.mark_line(strokeWidth=2, strokeCap="round", color=single_color).encode(
            x=x_enc, y=alt.Y(f"{y}:Q", title=y_title))
        wide = df[[x, y]]
        tooltip = [x_tip, alt.Tooltip(f"{y}:Q", title=y_title, format=y_fmt)]

    points = line.mark_point(filled=True, size=64, opacity=1).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)))
    rule = alt.Chart(wide)
    if hour_of_day:
        rule = rule.transform_calculate(clock=clock.format(v=x))
    rule = (
        rule.mark_rule(color=MUTED, strokeWidth=1)
        .encode(x=f"{x}:{x_type}", opacity=alt.condition(hover, alt.value(0.7), alt.value(0)), tooltip=tooltip)
        .add_params(hover)
    )
    return alt.layer(line, points, rule).properties(height=height)


def zero_line(chart: alt.Chart) -> alt.LayerChart:
    """Adds a baseline at 0, for values that can be negative."""
    zero = alt.Chart(pd.DataFrame({"zero": [0]})).mark_rule(color=MUTED, strokeWidth=1).encode(y="zero:Q")
    return alt.layer(chart, zero)


def scatter_with_fit(
    df: pd.DataFrame,
    x: str,
    y: str,
    x_title: str,
    y_title: str,
    color: str,
    fit_line: Optional[Dict[str, float]],
    tooltip_extra: List[alt.Tooltip] = (),
    height: int = 300,
) -> alt.Chart:
    """One point per day, with the least-squares line through them."""
    points = alt.Chart(df).mark_circle(size=64, opacity=0.55, color=color).encode(
        x=alt.X(f"{x}:Q", title=x_title, scale=alt.Scale(zero=False)),
        y=alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("local_date:T", title="Day", format="%a %d %b %Y"),
                 alt.Tooltip(f"{x}:Q", title=x_title, format=".1f"),
                 alt.Tooltip(f"{y}:Q", title=y_title, format=".1f"), *tooltip_extra],
    )
    if not fit_line or df.empty:
        return points.properties(height=height)
    lo, hi = df[x].min(), df[x].max()
    line_df = pd.DataFrame({x: [lo, hi], y: [fit_line["intercept"] + fit_line["slope"] * v for v in (lo, hi)]})
    line = alt.Chart(line_df).mark_line(strokeWidth=2, color=MUTED).encode(x=f"{x}:Q", y=f"{y}:Q")
    return alt.layer(points, line).properties(height=height)


def columns(
    df: pd.DataFrame,
    x: str,
    y: str,
    y_title: str,
    color: str,
    x_format: str = "%b %Y",
    tooltip_extra: List[alt.Tooltip] = (),
    height: int = 260,
) -> alt.LayerChart:
    """Columns from a zero baseline; values may be negative. Empty values leave a gap."""
    bars = alt.Chart(df).mark_bar(color=color, cornerRadiusEnd=4, size=12).encode(
        x=alt.X(f"{x}:T", title=None, axis=alt.Axis(format=x_format, labelOverlap=True, tickCount=MONTH_TICKS)),
        y=alt.Y(f"{y}:Q", title=y_title),
        tooltip=[alt.Tooltip(f"{x}:T", title="Month", format="%B %Y"),
                 alt.Tooltip(f"{y}:Q", title=y_title, format=".2f"), *tooltip_extra],
    )
    return zero_line(bars.properties(height=height))


# The hours of one day (use case 1): the cheapest block in blue, the dearest hours in red, the rest in a
# light grey that recedes. Blue and red pass the palette validator's checks as a pair in both modes;
# the grey is set apart by lightness. The legend and the tooltip name every bar's role.
HOUR_ROLE_COLORS_LIGHT = ["#2a78d6", "#d03b3b", "#c3c2b7"]
HOUR_ROLE_COLORS_DARK = ["#3987e5", "#e66767", "#52514e"]


def hour_bars(df: pd.DataFrame, roles: Sequence[str], y_title: str, height: int = 300) -> alt.LayerChart:
    """One column per hour of a day, coloured by role (`roles`: cheapest, expensive, other, in that order).

    df needs time (unique label per hour, in order), price_ore, role, spot_ore and with_vat_ore.
    """
    colors = HOUR_ROLE_COLORS_DARK if _dark() else HOUR_ROLE_COLORS_LIGHT
    bars = alt.Chart(df).mark_bar(cornerRadiusEnd=4, size=22).encode(
        x=alt.X("time:O", title=None, sort=list(df["time"]),
                axis=alt.Axis(labelAngle=0, values=[t for t in df["time"] if t[:2] in ("00", "06", "12", "18")])),
        y=alt.Y("price_ore:Q", title=y_title),
        color=alt.Color("role:N", scale=_scale(roles, colors), legend=alt.Legend(orient="top", title=None)),
        tooltip=[alt.Tooltip("time:O", title="Hour"),
                 alt.Tooltip("price_ore:Q", title=y_title, format=".1f"),
                 alt.Tooltip("role:N", title="This hour"),
                 alt.Tooltip("spot_ore:Q", title="Spot price, without VAT (øre/kWh)", format=".1f"),
                 alt.Tooltip("with_vat_ore:Q", title="With VAT, no support (øre/kWh)", format=".1f")],
    )
    return zero_line(bars.properties(height=height))
