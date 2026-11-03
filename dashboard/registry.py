"""
The list of pages the dashboard offers: the four Gold use cases (docs/plans/use_cases.md) and pages without a
use case number, such as the API's documentation (listed under "More").

The menu, the overview page and the placeholder pages are all built from this list. To add a use case:
  1. write dashboard/views/<name>.py with a `render()` function that draws the page;
  2. set the entry's `view` to "dashboard.views.<name>" and its `status` to READY.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

READY = "ready"
PLANNED = "planned"


@dataclass(frozen=True)
class UseCase:
    key: str  # part of the page URL, e.g. /prices-weather
    number: Optional[int]  # use case number in use_cases.md; None for shared pages
    kind: str  # the guide's consumer type: Dashboard, Application, Report, Analytical model
    title: str
    question: str
    user: str
    ready_by: str  # Norwegian time
    refresh: str
    gold_tables: Tuple[str, ...]
    status: str
    icon: str  # Streamlit material icon
    view: Optional[str] = None  # module with render(), for READY pages

    @property
    def label(self) -> str:
        return f"{self.number}. {self.title}" if self.number else self.title


USE_CASES = [
    UseCase(
        key="prices-weather",
        number=2,
        kind="Dashboard",
        title="Prices and weather",
        question="How do prices differ between zones, and how much do cold and wind move them?",
        user="Analyst, us in the report",
        ready_by="06:00",
        refresh="Daily, after the nightly pipeline run (02:17 UTC)",
        gold_tables=("zone_daily", "price_weather_sensitivity", "price_profile"),
        status=READY,
        icon=":material/monitoring:",
        view="dashboard.views.prices_weather",
    ),
    UseCase(
        key="cheapest-hours",
        number=1,
        kind="Application",
        title="Cheapest hours tomorrow",
        question="When should I charge the car or run the dishwasher tomorrow, and what will it cost?",
        user="Household, smart charger",
        ready_by="14:30",
        refresh="Daily, every 30 min after tomorrow's prices are published (~13:00)",
        gold_tables=("consumer_prices_hourly", "cheapest_hours"),
        status=READY,
        icon=":material/ev_station:",
        view="dashboard.views.cheapest_hours",
    ),
    UseCase(
        key="monthly-bill",
        number=3,
        kind="Report",
        title="Monthly household bill",
        question="What did a typical household pay last month, and was Norgespris cheaper than strømstønad?",
        user="Household, journalist",
        ready_by="06:00 on the 1st",
        refresh="Monthly, with a month-to-date row updated daily",
        gold_tables=("household_bill_monthly",),
        status=READY,
        icon=":material/receipt_long:",
        view="dashboard.views.household_bill",
    ),
    UseCase(
        key="price-outlook",
        number=4,
        kind="Analytical model",
        title="Price outlook",
        question="Roughly what will tomorrow's prices be, before they are published?",
        user="Anyone who plans before 13:00",
        ready_by="06:00",
        refresh="Daily; scored against the real prices at 14:30",
        gold_tables=("price_features", "price_outlook", "outlook_accuracy"),
        status=PLANNED,
        icon=":material/query_stats:",
    ),
    UseCase(
        key="api",
        number=None,
        kind="For developers",
        title="API for apps",
        question="How can a car charger or a home-automation system get the cheapest hours by itself?",
        user="Smart chargers, home automation, developers",
        ready_by="14:30 for tomorrow's prices",
        refresh="Follows Gold: the nightly and the afternoon runs",
        gold_tables=("consumer_prices_hourly", "cheapest_hours"),
        status=READY,
        icon=":material/api:",
        view="dashboard.views.api_docs",
    ),
]
