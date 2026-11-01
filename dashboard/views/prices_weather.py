"""
Use case 2, dashboard: prices and weather (docs/plans/use_cases.md, section 5.2).

"How do prices differ between zones, and how much do cold and wind move them?"
  * What happened: price per zone over time, the south-north gap, and a summary per zone.
  * Why: for one zone, daily price against temperature and wind, the monthly sensitivity Gold
    computed, and the price over the hours of the day.

Reads the Gold tables zone_daily, price_weather_sensitivity and price_profile.
"""

import altair as alt
import pandas as pd
import streamlit as st

from config.settings import PRICE_ZONES
from dashboard import charts, data, ui
from dashboard.registry import USE_CASES

TABLES = ["zone_daily", "price_weather_sensitivity", "price_profile"]
USE_CASE = next(uc for uc in USE_CASES if uc.key == "prices-weather")

SEASONS = {"Winter (Oct–Mar)": "winter", "Summer (Apr–Sep)": "summer", "Whole year": None}

# Periods covering this many months or fewer show the monthly sensitivity as numbers, not bars.
SHORT_PERIOD_MONTHS = 2


def _signed(value: float, unit: str) -> str:
    return f"{value:+.1f} {unit}".replace("-", "−")


def _sensitivity_box(col, label: str, ore, corr) -> None:
    """One month's sensitivity as a big number, or why there is none."""
    with col.container(border=True):
        if pd.isna(ore):
            st.metric(label, "–", help="Too few complete days, or the weather didn't vary.")
        else:
            st.metric(label, _signed(ore, "øre/kWh"), help="Slope of the daily price on the daily weather.")
            st.caption(f"Correlation {corr:.2f} ({'strong' if abs(corr) >= 0.7 else 'moderate' if abs(corr) >= 0.4 else 'weak'})")


def render() -> None:
    tables = ui.load_tables(TABLES)
    st.title(USE_CASE.label)
    st.markdown(f"*{USE_CASE.question}*  \n:blue-badge[{USE_CASE.kind}] For: {USE_CASE.user}. "
                "Spot prices exclude VAT and support schemes.")
    if tables is None:
        return
    daily, monthly, profile = tables["zone_daily"], tables["price_weather_sensitivity"], tables["price_profile"]
    if daily.empty:
        st.info("Gold has no days yet.")
        return
    names, colors = data.zone_names(), charts.zone_colors()

    # Filters, one row above everything they scope.
    first, last = daily["local_date"].min().date(), daily["local_date"].max().date()
    presets = data.range_presets(first, last)
    left, right = st.columns([3, 1])
    with left:
        preset = st.pills("Period", presets, default=data.PRESET_LAST_YEAR) or data.PRESET_LAST_YEAR
    with right:
        zone = st.selectbox("Zone for the weather analysis", PRICE_ZONES, format_func=lambda z: f"{z} {names[z]}")
    start, end = data.range_bounds(preset, first, last)
    period = data.in_range(daily, start, end)
    st.caption(f"{start:%d %b %Y} – {end:%d %b %Y}")

    # ------------------------------------------------------------------ #
    # What happened
    # ------------------------------------------------------------------ #
    summary = data.zone_summary(period)
    st.markdown("**Average spot price in the period** (øre/kWh, excluding VAT)")
    for col, row in zip(st.columns(len(summary)), summary.itertuples()):
        col.metric(row.bidding_zone, f"{row.avg_price_ore:.1f}",
                   help=f"{names[row.bidding_zone]}: the mean of the daily average spot prices in the period.")

    series = data.price_series(daily, monthly, start, end)
    monthly_view = data.use_monthly(start, end)
    st.subheader(f"{'Monthly' if monthly_view else 'Daily'} average spot price by zone")
    st.altair_chart(
        charts.lines(series, "date", "price_ore", "øre/kWh", series="bidding_zone", domain=PRICE_ZONES,
                     colors=list(colors.values()), x_format="%b %Y" if monthly_view else "%d %b %Y"),
        use_container_width=True,
    )
    with st.expander("Table: price by zone"):
        st.dataframe(series.pivot_table(index="date", columns="bidding_zone", values="price_ore").round(2),
                     use_container_width=True)

    st.subheader("South minus north: NO2 minus NO4")
    st.caption("How much more a kWh cost in the south (Kristiansand) than in the north (Tromsø). "
               "Above zero, the south was dearer.")
    gap = data.spread(series)
    st.altair_chart(
        charts.zero_line(charts.lines(gap, "date", "spread_ore", "øre/kWh", single_color=colors["NO2"],
                                      x_format="%b %Y" if monthly_view else "%d %b %Y", height=220)),
        use_container_width=True,
    )

    st.subheader("Zone summary")
    st.dataframe(
        summary,
        hide_index=True,
        use_container_width=True,
        column_config={
            "bidding_zone": "Zone",
            "avg_price_ore": st.column_config.NumberColumn("Avg price (øre/kWh)", format="%.1f"),
            "min_price_ore": st.column_config.NumberColumn("Lowest hour", format="%.1f"),
            "max_price_ore": st.column_config.NumberColumn("Highest hour", format="%.1f"),
            "volatility_ore": st.column_config.NumberColumn(
                "Volatility", format="%.1f", help="Average daily standard deviation of the hourly price (øre/kWh)"),
            "peak_offpeak_ratio": st.column_config.NumberColumn(
                "Peak / off-peak", format="%.2f", help="Workdays 08–20 against the other hours"),
            "negative_price_hours": st.column_config.NumberColumn("Negative hours", format="%d"),
            "avg_temp_celsius": st.column_config.NumberColumn("Avg temp (°C)", format="%.1f"),
            "hdd": st.column_config.NumberColumn("Heating degree days", format="%.0f",
                                                 help="Sum of max(0, 17 °C − temperature) per hour, divided by 24"),
            "avg_wind_mps": st.column_config.NumberColumn("Avg wind (m/s)", format="%.1f"),
            "complete_days": st.column_config.NumberColumn("Complete days", format="%d",
                                                           help="Days where every hour has a price and a temperature"),
            "days": st.column_config.NumberColumn("Days", format="%d"),
        },
    )

    # ------------------------------------------------------------------ #
    # Why: weather
    # ------------------------------------------------------------------ #
    st.divider()
    st.header(f"How weather moves the price in {zone} · {names[zone]}")
    zone_days = period[(period["bidding_zone"] == zone) & period["is_complete"]].assign(
        price_ore=lambda d: d["avg_price_nok_kwh"] * 100)
    st.caption(f"Each point is one complete day ({len(zone_days)} in the period). Days with missing hours "
               "are left out, as in Gold's monthly figures. The grey line is the best straight-line fit.")

    temp_col, wind_col = st.columns(2)
    with temp_col:
        st.markdown("**Daily temperature against price**")
        temp_fit = data.fit(zone_days["avg_temp_celsius"], zone_days["price_ore"])
        st.altair_chart(charts.scatter_with_fit(zone_days, "avg_temp_celsius", "price_ore", "Temperature (°C)",
                                                "Price (øre/kWh)", colors[zone], temp_fit),
                        use_container_width=True)
        if temp_fit:
            st.caption(f"Each degree colder went with {_signed(-temp_fit['slope'], 'øre/kWh')} "
                       f"(correlation {temp_fit['corr']:.2f}, {temp_fit['n']} days).")
    with wind_col:
        st.markdown("**Daily wind against price**")
        wind_fit = data.fit(zone_days["avg_wind_mps"], zone_days["price_ore"])
        st.altair_chart(charts.scatter_with_fit(zone_days.dropna(subset=["avg_wind_mps"]), "avg_wind_mps",
                                                "price_ore", "Wind (m/s)", "Price (øre/kWh)", colors[zone], wind_fit),
                        use_container_width=True)
        if wind_fit:
            st.caption(f"Each extra m/s of wind went with {_signed(wind_fit['slope'], 'øre/kWh')} "
                       f"(correlation {wind_fit['corr']:.2f}, {wind_fit['n']} days).")

    zone_months = data.months_in_range(monthly[monthly["bidding_zone"] == zone], start, end).sort_values("month_start")
    st.markdown("**Month by month: how strongly the price followed the weather**")
    st.caption("Computed in Gold for each month from its complete days: how much the daily price changed per "
               "degree colder and per extra m/s of wind. Months with fewer than 10 complete days are left empty.")
    if len(zone_months) <= SHORT_PERIOD_MONTHS:
        # One or two months: bars would be a lonely column each, so show the numbers instead.
        for month in zone_months.itertuples():
            st.caption(f"{month.month_start:%B %Y} · {month.fit_days} complete days")
            temp_box, wind_box = st.columns(2)
            _sensitivity_box(temp_box, "Each degree colder", month.ore_per_degree_colder, month.corr_temp_price)
            _sensitivity_box(wind_box, "Each extra m/s of wind", month.ore_per_mps_wind, month.corr_wind_price)
    else:
        extra = [alt.Tooltip("fit_days:Q", title="Complete days")]
        sens_temp, sens_wind = st.columns(2)
        with sens_temp:
            st.altair_chart(charts.columns(zone_months, "month_start", "ore_per_degree_colder",
                                           "øre/kWh per °C colder", colors[zone], tooltip_extra=extra +
                                           [alt.Tooltip("corr_temp_price:Q", title="Correlation", format=".2f")]),
                            use_container_width=True)
        with sens_wind:
            st.altair_chart(charts.columns(zone_months, "month_start", "ore_per_mps_wind",
                                           "øre/kWh per extra m/s wind", colors[zone], tooltip_extra=extra +
                                           [alt.Tooltip("corr_wind_price:Q", title="Correlation", format=".2f")]),
                            use_container_width=True)
    with st.expander("Table: monthly figures"):
        st.dataframe(
            zone_months[["month_start", "avg_price_nok_kwh", "avg_temp_celsius", "avg_wind_mps", "hdd",
                         "negative_price_hours", "fit_days", "corr_temp_price", "ore_per_degree_colder",
                         "corr_wind_price", "ore_per_mps_wind"]]
            .assign(avg_price_nok_kwh=lambda d: d["avg_price_nok_kwh"] * 100)
            .rename(columns={"month_start": "Month", "avg_price_nok_kwh": "Avg price (øre/kWh)"}),
            hide_index=True, use_container_width=True,
            column_config={"Month": st.column_config.DateColumn(format="MMM YYYY")},
        )

    # ------------------------------------------------------------------ #
    # Hours of the day
    # ------------------------------------------------------------------ #
    st.subheader(f"Price over the day in {zone}: workdays against weekends")
    season_label = st.segmented_control("Season", list(SEASONS), default="Winter (Oct–Mar)") or "Whole year"
    hours = data.profile_for(profile, zone, start, end, SEASONS[season_label])
    st.caption(f"Average per hour of the day (Norwegian time), over {start.year}–{end.year}"
               if start.year != end.year else f"Average per hour of the day (Norwegian time), {start.year}")
    if hours.empty:
        st.info("No hours for this season in the period.")
    else:
        day_types = [d for d in charts.DAY_TYPE_LABELS if d in set(hours["day_type"])]
        day_colors = charts.DAY_TYPE_COLORS_DARK if charts._dark() else charts.DAY_TYPE_COLORS_LIGHT
        st.altair_chart(
            charts.lines(hours, "local_hour", "price_ore", "øre/kWh", series="day_type", domain=day_types,
                         colors=day_colors[:len(day_types)], x_type="Q", x_title="Hour of the day",
                         series_labels=charts.DAY_TYPE_LABELS, hour_of_day=True),
            use_container_width=True,
        )

    with st.expander("Definitions"):
        st.markdown(
            "- **Spot price**: the day-ahead price per kWh, excluding VAT, grid tariff and support schemes.\n"
            "- **Peak hours**: 08:00–20:00 Norwegian time on workdays (not weekends or public holidays).\n"
            "- **Volatility**: the standard deviation of the 23–25 hourly prices within a day.\n"
            "- **Heating degree days**: for every hour, max(0, 17 °C − temperature), summed and divided by 24.\n"
            "- **Complete day**: every hour of the day (23, 24 or 25 with daylight saving) has a price and a "
            "temperature. Only complete days are used for correlations and slopes.\n"
            "- **Zone weather**: the mean of the zone's stations (NO2 averages Kjevik and Sola).\n"
            "- A correlation describes how prices and weather moved together, not that the weather caused it: "
            "hydropower reserves, gas prices and exports matter too."
        )
