"""
Household electricity price rules and the modelled household (docs/plans/use_cases.md, sections 5.3
and 9). Plain Python, so the Gold layer (src/gold/household_bill.py), the sample Gold generator and the
dashboard share one copy. Prices are NOK/kWh excluding VAT.

The rules were confirmed on regjeringen.no on 22 Oct 2026.
"""

from datetime import date

VAT = 0.25
NO_VAT_ZONES = ("NO4",)  # households in Nordland, Troms and Finnmark pay no VAT on electricity
STROMSTOTTE_SHARE = 0.9  # of each hour's spot price above the threshold
STROMSTOTTE_THRESHOLD_NOK = {2024: 0.73, 2025: 0.75, 2026: 0.77}  # per year
NORGESPRIS_NOK = 0.40
NORGESPRIS_FROM, NORGESPRIS_TO = date(2025, 10, 1), date(2026, 12, 31)
SUPPORT_CAP_KWH = 5000  # per month, under both schemes

# The bill starts with the first year the rules above cover.
BILL_START = date(2024, 1, 1)

# The household: every hour uses a base load plus heating that grows as it gets colder.
# kWh in an hour = BASE_LOAD_KWH + KWH_PER_HDH x heating degree hours.
BASE_LOAD_KWH = 0.6
KWH_PER_HDH = 0.1

SCHEME_NONE = "no_support"
SCHEME_STROMSTOTTE = "stromstotte"
SCHEME_NORGESPRIS = "norgespris"
SCHEMES = (SCHEME_NONE, SCHEME_STROMSTOTTE, SCHEME_NORGESPRIS)
