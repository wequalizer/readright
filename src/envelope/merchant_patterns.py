"""Merchant pattern matching for bank transaction categorization.

Maps raw bank statement descriptions to clean merchant names and categories.
Patterns are sourced from real bank statement descriptions across major banks
(ING, Rabobank, ABN AMRO, Chase, Revolut, Bunq, N26, etc.).

Pattern types:
  - prefix:   description starts with this string (case-insensitive)
  - contains: description contains this string anywhere (case-insensitive)
  - regex:    full regex match against description (case-insensitive)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class MerchantMatch:
    merchant: str
    category: str
    original_description: str


# ──────────────────────────────────────────────────────────────────────────────
# MERCHANT_PATTERNS — ordered by specificity (more specific patterns first)
#
# Each entry:
#   pattern  — the string or regex to match
#   match    — "prefix" | "contains" | "regex"
#   merchant — clean merchant name
#   category — spending category
#
# Categories used:
#   Groceries, Supermarket, Shopping, Entertainment, Subscriptions,
#   Transport, Dining, Coffee, Fuel, Health & Pharmacy, Home & Garden,
#   Clothing & Fashion, Electronics, Travel & Accommodation, Insurance,
#   Utilities, Education, Financial Services, Food Delivery,
#   Government & Tax, Donations, Personal Care, Sports & Fitness,
#   Pets, Kids, Telecom, ATM & Cash
# ──────────────────────────────────────────────────────────────────────────────

MERCHANT_PATTERNS: list[dict] = [

    # ── AMAZON (multiple descriptor variants per card network / region) ────
    {"pattern": "AMZN MKTP",          "match": "prefix",   "merchant": "Amazon Marketplace",    "category": "Shopping"},
    {"pattern": "AMZN*MKTP",          "match": "prefix",   "merchant": "Amazon Marketplace",    "category": "Shopping"},
    {"pattern": "AMZN DIGITAL",       "match": "prefix",   "merchant": "Amazon Digital",        "category": "Shopping"},
    {"pattern": "AMZN*DIGITAL",       "match": "prefix",   "merchant": "Amazon Digital",        "category": "Shopping"},
    {"pattern": "AMZN PRIME",         "match": "prefix",   "merchant": "Amazon Prime",          "category": "Subscriptions"},
    {"pattern": "AMZN*PRIME",         "match": "prefix",   "merchant": "Amazon Prime",          "category": "Subscriptions"},
    {"pattern": "PRIME VIDEO",        "match": "prefix",   "merchant": "Amazon Prime Video",    "category": "Subscriptions"},
    {"pattern": "AMZ*",               "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMZN*",              "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMAZON MKTPLACE",    "match": "prefix",   "merchant": "Amazon Marketplace",    "category": "Shopping"},
    {"pattern": "AMAZON.COM",         "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMAZON.CO",          "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMAZON.NL",          "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMAZON.DE",          "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMAZON EU",          "match": "prefix",   "merchant": "Amazon",                "category": "Shopping"},
    {"pattern": "AMAZON FRESH",       "match": "prefix",   "merchant": "Amazon Fresh",          "category": "Groceries"},
    {"pattern": "AMAZON WEB SERVICES","match": "prefix",   "merchant": "AWS",                   "category": "Subscriptions"},
    {"pattern": "AWS EMEA",           "match": "prefix",   "merchant": "AWS",                   "category": "Subscriptions"},

    # ── PAYPAL (prefix PP* or PAYPAL * followed by merchant) ──────────────
    {"pattern": "PAYPAL *SPOTIFY",    "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "PP*SPOTIFY",         "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "PAYPAL *NETFLIX",    "match": "prefix",   "merchant": "Netflix",               "category": "Subscriptions"},
    {"pattern": "PP*NETFLIX",         "match": "prefix",   "merchant": "Netflix",               "category": "Subscriptions"},
    {"pattern": "PAYPAL *STEAM",      "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "PP*STEAM",           "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "PAYPAL *EBAY",       "match": "prefix",   "merchant": "eBay",                  "category": "Shopping"},
    {"pattern": "PP*EBAY",            "match": "prefix",   "merchant": "eBay",                  "category": "Shopping"},
    {"pattern": "PAYPAL *DROPBOX",    "match": "prefix",   "merchant": "Dropbox",               "category": "Subscriptions"},
    {"pattern": "PP*DROPBOX",         "match": "prefix",   "merchant": "Dropbox",               "category": "Subscriptions"},
    {"pattern": "PAYPALINST XFER",    "match": "prefix",   "merchant": "PayPal Transfer",       "category": "Financial Services"},
    {"pattern": "PYPL PAYMTHLY",      "match": "prefix",   "merchant": "PayPal Pay Monthly",    "category": "Financial Services"},
    {"pattern": "PAYPAL *",           "match": "prefix",   "merchant": "PayPal",                "category": "Shopping"},
    {"pattern": "PP*",                "match": "prefix",   "merchant": "PayPal",                "category": "Shopping"},

    # ── GOOGLE ────────────────────────────────────────────────────────────
    {"pattern": "GOOGLE*YOUTUBE PREMIUM", "match": "prefix", "merchant": "YouTube Premium",     "category": "Subscriptions"},
    {"pattern": "GOOGLE *YOUTUBE",    "match": "prefix",   "merchant": "YouTube Premium",       "category": "Subscriptions"},
    {"pattern": "GOOGLE*SPOTIFY",     "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "GOOGLE *SPOTIFY",    "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "GOOGLE*BUMBLE",      "match": "prefix",   "merchant": "Bumble",                "category": "Subscriptions"},
    {"pattern": "GOOGLE PLAY*BUMBLE", "match": "prefix",   "merchant": "Bumble",                "category": "Subscriptions"},
    {"pattern": "GOOGLE*GOOGLE ONE",  "match": "prefix",   "merchant": "Google One",            "category": "Subscriptions"},
    {"pattern": "GOOGLE *GOOGLE ONE", "match": "prefix",   "merchant": "Google One",            "category": "Subscriptions"},
    {"pattern": "GOOGLE*GOOGLE STORAGE","match": "prefix", "merchant": "Google One",            "category": "Subscriptions"},
    {"pattern": "GOOGLE*SERVICES",    "match": "prefix",   "merchant": "Google Services",       "category": "Subscriptions"},
    {"pattern": "GOOGLE *SERVICES",   "match": "prefix",   "merchant": "Google Services",       "category": "Subscriptions"},
    {"pattern": "GOOGLE*",            "match": "prefix",   "merchant": "Google",                "category": "Shopping"},
    {"pattern": "GOOGLE IRELAND",     "match": "prefix",   "merchant": "Google",                "category": "Shopping"},

    # ── APPLE ─────────────────────────────────────────────────────────────
    {"pattern": "APPLE.COM/BILL",     "match": "prefix",   "merchant": "Apple",                 "category": "Subscriptions"},
    {"pattern": "APPLE.COM/US",       "match": "prefix",   "merchant": "Apple Store",           "category": "Shopping"},
    {"pattern": "APPLE.COM/NL",       "match": "prefix",   "merchant": "Apple Store",           "category": "Shopping"},
    {"pattern": "APL*ITUNES",         "match": "prefix",   "merchant": "Apple iTunes",          "category": "Subscriptions"},
    {"pattern": "APL* ITUNES",        "match": "prefix",   "merchant": "Apple iTunes",          "category": "Subscriptions"},
    {"pattern": "ITUNES.COM/BILL",    "match": "prefix",   "merchant": "Apple iTunes",          "category": "Subscriptions"},
    {"pattern": "APPLE STORE",        "match": "prefix",   "merchant": "Apple Store",           "category": "Electronics"},
    {"pattern": "APPLE ONLINE",       "match": "prefix",   "merchant": "Apple Store",           "category": "Electronics"},

    # ── MICROSOFT ─────────────────────────────────────────────────────────
    {"pattern": "MSFT*MICROSOFT 365", "match": "prefix",   "merchant": "Microsoft 365",         "category": "Subscriptions"},
    {"pattern": "MICROSOFT*365",      "match": "prefix",   "merchant": "Microsoft 365",         "category": "Subscriptions"},
    {"pattern": "MICROSOFT *XBOX",    "match": "prefix",   "merchant": "Xbox",                  "category": "Entertainment"},
    {"pattern": "MSFT*XBOX",          "match": "prefix",   "merchant": "Xbox",                  "category": "Entertainment"},
    {"pattern": "XBOX",               "match": "prefix",   "merchant": "Xbox",                  "category": "Entertainment"},
    {"pattern": "MICROSOFT*LINKEDIN", "match": "prefix",   "merchant": "LinkedIn Premium",      "category": "Subscriptions"},
    {"pattern": "MSFT*LINKEDIN",      "match": "prefix",   "merchant": "LinkedIn Premium",      "category": "Subscriptions"},
    {"pattern": "MSFT*GITHUB",        "match": "prefix",   "merchant": "GitHub",                "category": "Subscriptions"},
    {"pattern": "MSBILL.INFO",        "match": "contains", "merchant": "Microsoft",             "category": "Subscriptions"},
    {"pattern": "MSFT*",              "match": "prefix",   "merchant": "Microsoft",             "category": "Subscriptions"},
    {"pattern": "MICROSOFT SERVICES", "match": "prefix",   "merchant": "Microsoft",             "category": "Subscriptions"},

    # ── STREAMING / ENTERTAINMENT ─────────────────────────────────────────
    {"pattern": "NETFLIX.COM",        "match": "prefix",   "merchant": "Netflix",               "category": "Subscriptions"},
    {"pattern": "NETFLIX",            "match": "prefix",   "merchant": "Netflix",               "category": "Subscriptions"},
    {"pattern": "NFLX",               "match": "prefix",   "merchant": "Netflix",               "category": "Subscriptions"},
    {"pattern": "SPOTIFY AB",         "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "SPOTIFY USA",        "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "SPOTIFY",            "match": "prefix",   "merchant": "Spotify",               "category": "Subscriptions"},
    {"pattern": "DISNEYPLUS.COM",     "match": "prefix",   "merchant": "Disney+",               "category": "Subscriptions"},
    {"pattern": "DISNEYPLUS",         "match": "prefix",   "merchant": "Disney+",               "category": "Subscriptions"},
    {"pattern": "DISNEY PLUS",        "match": "prefix",   "merchant": "Disney+",               "category": "Subscriptions"},
    {"pattern": "HBOMAX.COM",         "match": "prefix",   "merchant": "Max",                   "category": "Subscriptions"},
    {"pattern": "HBOMAX",             "match": "prefix",   "merchant": "Max",                   "category": "Subscriptions"},
    {"pattern": "HBO MAX",            "match": "prefix",   "merchant": "Max",                   "category": "Subscriptions"},
    {"pattern": "MAX.COM",            "match": "prefix",   "merchant": "Max",                   "category": "Subscriptions"},
    {"pattern": "YOUTUBE PREMIUM",    "match": "prefix",   "merchant": "YouTube Premium",       "category": "Subscriptions"},
    {"pattern": "CRUNCHYROLL",        "match": "prefix",   "merchant": "Crunchyroll",           "category": "Subscriptions"},
    {"pattern": "APPLE TV",           "match": "prefix",   "merchant": "Apple TV+",             "category": "Subscriptions"},
    {"pattern": "APPLE MUSIC",        "match": "prefix",   "merchant": "Apple Music",           "category": "Subscriptions"},
    {"pattern": "AUDIBLE",            "match": "prefix",   "merchant": "Audible",               "category": "Subscriptions"},
    {"pattern": "PARAMOUNT+",         "match": "prefix",   "merchant": "Paramount+",            "category": "Subscriptions"},
    {"pattern": "PARAMOUNT PLUS",     "match": "prefix",   "merchant": "Paramount+",            "category": "Subscriptions"},
    {"pattern": "VIDEOLAND",          "match": "prefix",   "merchant": "Videoland",             "category": "Subscriptions"},

    # ── GAMING ────────────────────────────────────────────────────────────
    {"pattern": "STEAMGAMES.COM",     "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "STEAMPOWERED.COM",   "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "STEAM PURCHASE",     "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "STEAM GAMES",        "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "VALVE SOFTWARE",     "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "VALVE CORPORATION",  "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "WL STEAM",           "match": "prefix",   "merchant": "Steam",                 "category": "Entertainment"},
    {"pattern": "NINTENDO",           "match": "prefix",   "merchant": "Nintendo",              "category": "Entertainment"},
    {"pattern": "PLAYSTATION",        "match": "prefix",   "merchant": "PlayStation",           "category": "Entertainment"},
    {"pattern": "SONY INTERACTIVE",   "match": "prefix",   "merchant": "PlayStation",           "category": "Entertainment"},
    {"pattern": "EPICGAMES",          "match": "prefix",   "merchant": "Epic Games",            "category": "Entertainment"},
    {"pattern": "EPIC GAMES",         "match": "prefix",   "merchant": "Epic Games",            "category": "Entertainment"},
    {"pattern": "BLIZZARD",           "match": "prefix",   "merchant": "Blizzard",              "category": "Entertainment"},
    {"pattern": "BATTLE.NET",         "match": "prefix",   "merchant": "Blizzard",              "category": "Entertainment"},

    # ── UBER ──────────────────────────────────────────────────────────────
    {"pattern": "UBER *EATS",         "match": "prefix",   "merchant": "Uber Eats",             "category": "Food Delivery"},
    {"pattern": "UBER* EATS",         "match": "prefix",   "merchant": "Uber Eats",             "category": "Food Delivery"},
    {"pattern": "UBER*EATS",          "match": "prefix",   "merchant": "Uber Eats",             "category": "Food Delivery"},
    {"pattern": "UBEREATS",           "match": "prefix",   "merchant": "Uber Eats",             "category": "Food Delivery"},
    {"pattern": "UBER *TRIP",         "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "UBER* TRIP",         "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "UBER *PENDING",      "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "UBER *ONE",          "match": "prefix",   "merchant": "Uber One",              "category": "Subscriptions"},
    {"pattern": "UBER *HELP",         "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "UBER BV",            "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "UBER TECHNOLOGIES",  "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "UBER.COM",           "match": "prefix",   "merchant": "Uber",                  "category": "Transport"},
    {"pattern": "PMNTSBVEATS",        "match": "prefix",   "merchant": "Uber Eats",             "category": "Food Delivery"},
    {"pattern": "PMNTSBVEAT",         "match": "prefix",   "merchant": "Uber Eats",             "category": "Food Delivery"},

    # ── FOOD DELIVERY ─────────────────────────────────────────────────────
    {"pattern": "THUISBEZORGD",       "match": "prefix",   "merchant": "Thuisbezorgd",          "category": "Food Delivery"},
    {"pattern": "TAKEAWAY.COM",       "match": "prefix",   "merchant": "Thuisbezorgd",          "category": "Food Delivery"},
    {"pattern": "JUST EAT",           "match": "prefix",   "merchant": "Just Eat",              "category": "Food Delivery"},
    {"pattern": "JUST-EAT",           "match": "prefix",   "merchant": "Just Eat",              "category": "Food Delivery"},
    {"pattern": "DELIVEROO",          "match": "prefix",   "merchant": "Deliveroo",             "category": "Food Delivery"},
    {"pattern": "DOORDASH",           "match": "prefix",   "merchant": "DoorDash",              "category": "Food Delivery"},
    {"pattern": "GRUBHUB",            "match": "prefix",   "merchant": "Grubhub",               "category": "Food Delivery"},
    {"pattern": "GORILLAS",           "match": "prefix",   "merchant": "Gorillas",              "category": "Groceries"},
    {"pattern": "GETIR",              "match": "prefix",   "merchant": "Getir",                 "category": "Groceries"},
    {"pattern": "FLINK",              "match": "prefix",   "merchant": "Flink",                 "category": "Groceries"},
    {"pattern": "PICNIC",             "match": "prefix",   "merchant": "Picnic",                "category": "Groceries"},

    # ── DUTCH SUPERMARKETS ────────────────────────────────────────────────
    {"pattern": "ALBERT HEIJN",       "match": "prefix",   "merchant": "Albert Heijn",          "category": "Groceries"},
    {"pattern": "AH TO GO",           "match": "prefix",   "merchant": "AH to Go",              "category": "Groceries"},
    {"pattern": "AH BEZORG",          "match": "prefix",   "merchant": "Albert Heijn",          "category": "Groceries"},
    {"pattern": r"^AH\s",             "match": "regex",    "merchant": "Albert Heijn",          "category": "Groceries"},
    {"pattern": "JUMBO",              "match": "prefix",   "merchant": "Jumbo",                 "category": "Groceries"},
    {"pattern": "LIDL",               "match": "prefix",   "merchant": "Lidl",                  "category": "Groceries"},
    {"pattern": "ALDI",               "match": "prefix",   "merchant": "Aldi",                  "category": "Groceries"},
    {"pattern": "PLUS SUPERMARKT",    "match": "prefix",   "merchant": "Plus",                  "category": "Groceries"},
    {"pattern": "DIRK VAN DEN BROEK", "match": "prefix",   "merchant": "Dirk",                  "category": "Groceries"},
    {"pattern": "DIRK ",              "match": "prefix",   "merchant": "Dirk",                  "category": "Groceries"},
    {"pattern": "DEKAMARKT",          "match": "prefix",   "merchant": "DekaMarkt",             "category": "Groceries"},
    {"pattern": "COOP SUPERMARKT",    "match": "prefix",   "merchant": "Coop",                  "category": "Groceries"},
    {"pattern": "VOMAR",              "match": "prefix",   "merchant": "Vomar",                 "category": "Groceries"},
    {"pattern": "SPAR ",              "match": "prefix",   "merchant": "Spar",                  "category": "Groceries"},
    {"pattern": "HOOGVLIET",          "match": "prefix",   "merchant": "Hoogvliet",             "category": "Groceries"},
    {"pattern": "NETTORAMA",          "match": "prefix",   "merchant": "Nettorama",             "category": "Groceries"},

    # ── INTERNATIONAL SUPERMARKETS ────────────────────────────────────────
    {"pattern": "TESCO",              "match": "prefix",   "merchant": "Tesco",                 "category": "Groceries"},
    {"pattern": "SAINSBURY",          "match": "prefix",   "merchant": "Sainsbury's",           "category": "Groceries"},
    {"pattern": "WALMART",            "match": "prefix",   "merchant": "Walmart",               "category": "Groceries"},
    {"pattern": "WHOLE FOODS",        "match": "prefix",   "merchant": "Whole Foods",           "category": "Groceries"},
    {"pattern": "TRADER JOE",         "match": "prefix",   "merchant": "Trader Joe's",          "category": "Groceries"},
    {"pattern": "CARREFOUR",          "match": "prefix",   "merchant": "Carrefour",             "category": "Groceries"},
    {"pattern": "REWE",               "match": "prefix",   "merchant": "REWE",                  "category": "Groceries"},
    {"pattern": "EDEKA",              "match": "prefix",   "merchant": "Edeka",                 "category": "Groceries"},
    {"pattern": "MERCADONA",          "match": "prefix",   "merchant": "Mercadona",             "category": "Groceries"},
    {"pattern": "COSTCO",             "match": "prefix",   "merchant": "Costco",                "category": "Groceries"},

    # ── DUTCH TRANSPORT ───────────────────────────────────────────────────
    {"pattern": "NS GROEP",           "match": "prefix",   "merchant": "NS (Dutch Railways)",   "category": "Transport"},
    {"pattern": "NS-",                "match": "prefix",   "merchant": "NS (Dutch Railways)",   "category": "Transport"},
    {"pattern": "NS REIZIGERS",       "match": "prefix",   "merchant": "NS (Dutch Railways)",   "category": "Transport"},
    {"pattern": "NS INT",             "match": "prefix",   "merchant": "NS International",      "category": "Transport"},
    {"pattern": "GVB",                "match": "prefix",   "merchant": "GVB",                   "category": "Transport"},
    {"pattern": "RET ",               "match": "prefix",   "merchant": "RET",                   "category": "Transport"},
    {"pattern": "HTM ",               "match": "prefix",   "merchant": "HTM",                   "category": "Transport"},
    {"pattern": "CONNEXXION",         "match": "prefix",   "merchant": "Connexxion",            "category": "Transport"},
    {"pattern": "ARRIVA",             "match": "prefix",   "merchant": "Arriva",                "category": "Transport"},
    {"pattern": "TRANSAVIA",          "match": "prefix",   "merchant": "Transavia",             "category": "Travel & Accommodation"},
    {"pattern": "OVPAY",              "match": "contains", "merchant": "OVpay",                 "category": "Transport"},
    {"pattern": "OV-CHIPKAART",       "match": "contains", "merchant": "OV-chipkaart",          "category": "Transport"},
    {"pattern": "SWAPFIETS",          "match": "prefix",   "merchant": "Swapfiets",             "category": "Transport"},
    {"pattern": "CHECK",              "match": "prefix",   "merchant": "Check",                 "category": "Transport"},

    # ── INTERNATIONAL TRANSPORT ───────────────────────────────────────────
    {"pattern": "LYFT",               "match": "prefix",   "merchant": "Lyft",                  "category": "Transport"},
    {"pattern": "BOLT.EU",            "match": "prefix",   "merchant": "Bolt",                  "category": "Transport"},
    {"pattern": "BOLT EU",            "match": "prefix",   "merchant": "Bolt",                  "category": "Transport"},
    {"pattern": "TIER MOBILITY",      "match": "prefix",   "merchant": "TIER",                  "category": "Transport"},
    {"pattern": "LIME",               "match": "prefix",   "merchant": "Lime",                  "category": "Transport"},
    {"pattern": "FELYX",              "match": "prefix",   "merchant": "Felyx",                 "category": "Transport"},
    {"pattern": "RYANAIR",            "match": "prefix",   "merchant": "Ryanair",               "category": "Travel & Accommodation"},
    {"pattern": "EASYJET",            "match": "prefix",   "merchant": "easyJet",               "category": "Travel & Accommodation"},
    {"pattern": "KLM",                "match": "prefix",   "merchant": "KLM",                   "category": "Travel & Accommodation"},
    {"pattern": "FLIXBUS",            "match": "prefix",   "merchant": "FlixBus",               "category": "Transport"},

    # ── DINING & FAST FOOD ────────────────────────────────────────────────
    {"pattern": "MCDONALD",           "match": "prefix",   "merchant": "McDonald's",            "category": "Dining"},
    {"pattern": "MCD'S",              "match": "prefix",   "merchant": "McDonald's",            "category": "Dining"},
    {"pattern": r"^MCD\s",            "match": "regex",    "merchant": "McDonald's",            "category": "Dining"},
    {"pattern": "BURGER KING",        "match": "prefix",   "merchant": "Burger King",           "category": "Dining"},
    {"pattern": "KFC ",               "match": "prefix",   "merchant": "KFC",                   "category": "Dining"},
    {"pattern": "KENTUCKY FRIED",     "match": "prefix",   "merchant": "KFC",                   "category": "Dining"},
    {"pattern": "SUBWAY",             "match": "prefix",   "merchant": "Subway",                "category": "Dining"},
    {"pattern": "DOMINO",             "match": "prefix",   "merchant": "Domino's",              "category": "Dining"},
    {"pattern": "NEW YORK PIZZA",     "match": "prefix",   "merchant": "New York Pizza",        "category": "Dining"},
    {"pattern": "FIVE GUYS",          "match": "prefix",   "merchant": "Five Guys",             "category": "Dining"},
    {"pattern": "VAPIANO",            "match": "prefix",   "merchant": "Vapiano",               "category": "Dining"},
    {"pattern": "WAGAMAMA",           "match": "prefix",   "merchant": "Wagamama",              "category": "Dining"},
    {"pattern": "FEBO",               "match": "prefix",   "merchant": "FEBO",                  "category": "Dining"},
    {"pattern": "SMULLERS",           "match": "prefix",   "merchant": "Smullers",              "category": "Dining"},
    {"pattern": "DUNKIN",             "match": "prefix",   "merchant": "Dunkin' Donuts",        "category": "Dining"},

    # ── COFFEE ────────────────────────────────────────────────────────────
    {"pattern": "STARBUCKS",          "match": "prefix",   "merchant": "Starbucks",             "category": "Coffee"},
    {"pattern": "SBX ",               "match": "prefix",   "merchant": "Starbucks",             "category": "Coffee"},
    {"pattern": "SBUX",               "match": "prefix",   "merchant": "Starbucks",             "category": "Coffee"},
    {"pattern": "STARBUCKS CARD",     "match": "prefix",   "merchant": "Starbucks",             "category": "Coffee"},
    {"pattern": "COSTA COFFEE",       "match": "prefix",   "merchant": "Costa Coffee",          "category": "Coffee"},
    {"pattern": "COFFEECOMPANY",      "match": "prefix",   "merchant": "Coffee Company",        "category": "Coffee"},
    {"pattern": "COFFEE COMPANY",     "match": "prefix",   "merchant": "Coffee Company",        "category": "Coffee"},
    {"pattern": "BAGELS & BEANS",     "match": "prefix",   "merchant": "Bagels & Beans",        "category": "Coffee"},

    # ── DUTCH RETAIL & SHOPPING ───────────────────────────────────────────
    {"pattern": "BOL.COM",            "match": "prefix",   "merchant": "Bol.com",               "category": "Shopping"},
    {"pattern": "BOL COM",            "match": "prefix",   "merchant": "Bol.com",               "category": "Shopping"},
    {"pattern": "COOLBLUE",           "match": "prefix",   "merchant": "Coolblue",              "category": "Electronics"},
    {"pattern": "MEDIAMARKT",         "match": "prefix",   "merchant": "MediaMarkt",            "category": "Electronics"},
    {"pattern": "MEDIA MARKT",        "match": "prefix",   "merchant": "MediaMarkt",            "category": "Electronics"},
    {"pattern": "HEMA",               "match": "prefix",   "merchant": "HEMA",                  "category": "Shopping"},
    {"pattern": "ACTION ",            "match": "prefix",   "merchant": "Action",                "category": "Shopping"},
    {"pattern": "BLOKKER",            "match": "prefix",   "merchant": "Blokker",               "category": "Shopping"},
    {"pattern": "KRUIDVAT",           "match": "prefix",   "merchant": "Kruidvat",              "category": "Health & Pharmacy"},
    {"pattern": "ETOS",               "match": "prefix",   "merchant": "Etos",                  "category": "Health & Pharmacy"},
    {"pattern": "PRAXIS",             "match": "prefix",   "merchant": "Praxis",                "category": "Home & Garden"},
    {"pattern": "GAMMA ",             "match": "prefix",   "merchant": "Gamma",                 "category": "Home & Garden"},
    {"pattern": "KARWEI",             "match": "prefix",   "merchant": "Karwei",                "category": "Home & Garden"},
    {"pattern": "HORNBACH",           "match": "prefix",   "merchant": "Hornbach",              "category": "Home & Garden"},
    {"pattern": "INTRATUIN",          "match": "prefix",   "merchant": "Intratuin",             "category": "Home & Garden"},
    {"pattern": "XENOS",              "match": "prefix",   "merchant": "Xenos",                 "category": "Shopping"},
    {"pattern": "FLYING TIGER",       "match": "prefix",   "merchant": "Flying Tiger",          "category": "Shopping"},
    {"pattern": "RITUALS",            "match": "prefix",   "merchant": "Rituals",               "category": "Personal Care"},
    {"pattern": "BIJENKORF",          "match": "prefix",   "merchant": "de Bijenkorf",          "category": "Shopping"},

    # ── INTERNATIONAL SHOPPING & RETAIL ───────────────────────────────────
    {"pattern": "IKEA",               "match": "prefix",   "merchant": "IKEA",                  "category": "Home & Garden"},
    {"pattern": "EBAY",               "match": "prefix",   "merchant": "eBay",                  "category": "Shopping"},
    {"pattern": "ALIEXPRESS",         "match": "prefix",   "merchant": "AliExpress",            "category": "Shopping"},
    {"pattern": "ALI EXPRESS",        "match": "prefix",   "merchant": "AliExpress",            "category": "Shopping"},
    {"pattern": "WISH.COM",           "match": "prefix",   "merchant": "Wish",                  "category": "Shopping"},
    {"pattern": "SHEIN",              "match": "prefix",   "merchant": "Shein",                 "category": "Clothing & Fashion"},
    {"pattern": "TEMU.COM",           "match": "prefix",   "merchant": "Temu",                  "category": "Shopping"},
    {"pattern": "TEMU ",              "match": "prefix",   "merchant": "Temu",                  "category": "Shopping"},
    {"pattern": "ETSY",               "match": "prefix",   "merchant": "Etsy",                  "category": "Shopping"},

    # ── CLOTHING & FASHION ────────────────────────────────────────────────
    {"pattern": "ZALANDO",            "match": "prefix",   "merchant": "Zalando",               "category": "Clothing & Fashion"},
    {"pattern": "WWW ZALANDO",        "match": "prefix",   "merchant": "Zalando",               "category": "Clothing & Fashion"},
    {"pattern": "H&M",                "match": "prefix",   "merchant": "H&M",                   "category": "Clothing & Fashion"},
    {"pattern": "H & M",              "match": "prefix",   "merchant": "H&M",                   "category": "Clothing & Fashion"},
    {"pattern": "HENNES & MAURITZ",   "match": "prefix",   "merchant": "H&M",                   "category": "Clothing & Fashion"},
    {"pattern": "ZARA ",              "match": "prefix",   "merchant": "Zara",                  "category": "Clothing & Fashion"},
    {"pattern": "ZARA.COM",           "match": "prefix",   "merchant": "Zara",                  "category": "Clothing & Fashion"},
    {"pattern": "ZARA INTERNET",      "match": "prefix",   "merchant": "Zara",                  "category": "Clothing & Fashion"},
    {"pattern": "PRIMARK",            "match": "prefix",   "merchant": "Primark",               "category": "Clothing & Fashion"},
    {"pattern": "UNIQLO",             "match": "prefix",   "merchant": "Uniqlo",                "category": "Clothing & Fashion"},
    {"pattern": "C&A",                "match": "prefix",   "merchant": "C&A",                   "category": "Clothing & Fashion"},
    {"pattern": "NIKE",               "match": "prefix",   "merchant": "Nike",                  "category": "Clothing & Fashion"},
    {"pattern": "ADIDAS",             "match": "prefix",   "merchant": "Adidas",                "category": "Clothing & Fashion"},
    {"pattern": "DECATHLON",          "match": "prefix",   "merchant": "Decathlon",             "category": "Sports & Fitness"},
    {"pattern": "SCOTCH & SODA",      "match": "prefix",   "merchant": "Scotch & Soda",         "category": "Clothing & Fashion"},
    {"pattern": "WE FASHION",         "match": "prefix",   "merchant": "WE Fashion",            "category": "Clothing & Fashion"},

    # ── FUEL / PETROL ─────────────────────────────────────────────────────
    {"pattern": "SHELL",              "match": "prefix",   "merchant": "Shell",                 "category": "Fuel"},
    {"pattern": "BP ",                "match": "prefix",   "merchant": "BP",                    "category": "Fuel"},
    {"pattern": "TOTALENERGIES",      "match": "prefix",   "merchant": "TotalEnergies",         "category": "Fuel"},
    {"pattern": "TOTAL ",             "match": "prefix",   "merchant": "TotalEnergies",         "category": "Fuel"},
    {"pattern": "ESSO",               "match": "prefix",   "merchant": "Esso",                  "category": "Fuel"},
    {"pattern": "TEXACO",             "match": "prefix",   "merchant": "Texaco",                "category": "Fuel"},
    {"pattern": "TANGO ",             "match": "prefix",   "merchant": "Tango",                 "category": "Fuel"},
    {"pattern": "TINQ",               "match": "prefix",   "merchant": "TinQ",                  "category": "Fuel"},
    {"pattern": "CHEVRON",            "match": "prefix",   "merchant": "Chevron",               "category": "Fuel"},
    {"pattern": "EXXON",              "match": "prefix",   "merchant": "Exxon",                 "category": "Fuel"},

    # ── TRAVEL & ACCOMMODATION ────────────────────────────────────────────
    {"pattern": "BOOKING.COM",        "match": "prefix",   "merchant": "Booking.com",           "category": "Travel & Accommodation"},
    {"pattern": "BOOKING COM",        "match": "prefix",   "merchant": "Booking.com",           "category": "Travel & Accommodation"},
    {"pattern": "AIRBNB",             "match": "prefix",   "merchant": "Airbnb",                "category": "Travel & Accommodation"},
    {"pattern": "HOTELS.COM",         "match": "prefix",   "merchant": "Hotels.com",            "category": "Travel & Accommodation"},
    {"pattern": "EXPEDIA",            "match": "prefix",   "merchant": "Expedia",               "category": "Travel & Accommodation"},
    {"pattern": "TRIVAGO",            "match": "prefix",   "merchant": "Trivago",               "category": "Travel & Accommodation"},
    {"pattern": "HOSTELWORLD",        "match": "prefix",   "merchant": "Hostelworld",           "category": "Travel & Accommodation"},

    # ── SOFTWARE / SAAS SUBSCRIPTIONS ─────────────────────────────────────
    {"pattern": "ADOBE SYSTEMS",      "match": "prefix",   "merchant": "Adobe",                 "category": "Subscriptions"},
    {"pattern": "ADOBE *CREATIVE",    "match": "prefix",   "merchant": "Adobe Creative Cloud",  "category": "Subscriptions"},
    {"pattern": "ADOBE*",             "match": "prefix",   "merchant": "Adobe",                 "category": "Subscriptions"},
    {"pattern": "DROPBOX",            "match": "prefix",   "merchant": "Dropbox",               "category": "Subscriptions"},
    {"pattern": "DBXDROPBOX",         "match": "prefix",   "merchant": "Dropbox",               "category": "Subscriptions"},
    {"pattern": "LINKEDIN PREMIUM",   "match": "prefix",   "merchant": "LinkedIn Premium",      "category": "Subscriptions"},
    {"pattern": "LINKEDIN CORP",      "match": "prefix",   "merchant": "LinkedIn",              "category": "Subscriptions"},
    {"pattern": "LINKEDIN.COM",       "match": "prefix",   "merchant": "LinkedIn",              "category": "Subscriptions"},
    {"pattern": "LI PREMIUM",         "match": "prefix",   "merchant": "LinkedIn Premium",      "category": "Subscriptions"},
    {"pattern": "GITHUB",             "match": "prefix",   "merchant": "GitHub",                "category": "Subscriptions"},
    {"pattern": "NOTION.SO",          "match": "prefix",   "merchant": "Notion",                "category": "Subscriptions"},
    {"pattern": "SLACK TECHNOLOG",    "match": "prefix",   "merchant": "Slack",                 "category": "Subscriptions"},
    {"pattern": "ZOOM.US",            "match": "prefix",   "merchant": "Zoom",                  "category": "Subscriptions"},
    {"pattern": "ZOOM VIDEO",         "match": "prefix",   "merchant": "Zoom",                  "category": "Subscriptions"},
    {"pattern": "CANVA",              "match": "prefix",   "merchant": "Canva",                 "category": "Subscriptions"},
    {"pattern": "OPENAI",             "match": "prefix",   "merchant": "OpenAI",                "category": "Subscriptions"},
    {"pattern": "CHATGPT",            "match": "prefix",   "merchant": "OpenAI",                "category": "Subscriptions"},
    {"pattern": "ANTHROPIC",          "match": "prefix",   "merchant": "Anthropic",             "category": "Subscriptions"},
    {"pattern": "GRAMMARLY",          "match": "prefix",   "merchant": "Grammarly",             "category": "Subscriptions"},
    {"pattern": "NORDVPN",            "match": "prefix",   "merchant": "NordVPN",               "category": "Subscriptions"},
    {"pattern": "EXPRESSVPN",         "match": "prefix",   "merchant": "ExpressVPN",            "category": "Subscriptions"},
    {"pattern": "1PASSWORD",          "match": "prefix",   "merchant": "1Password",             "category": "Subscriptions"},
    {"pattern": "BITWARDEN",          "match": "prefix",   "merchant": "Bitwarden",             "category": "Subscriptions"},
    {"pattern": "CLOUDFLARE",         "match": "prefix",   "merchant": "Cloudflare",            "category": "Subscriptions"},
    {"pattern": "DIGITALOCEAN",       "match": "prefix",   "merchant": "DigitalOcean",          "category": "Subscriptions"},
    {"pattern": "HETZNER",            "match": "prefix",   "merchant": "Hetzner",               "category": "Subscriptions"},

    # ── DATING ────────────────────────────────────────────────────────────
    {"pattern": "TINDER.COM",         "match": "prefix",   "merchant": "Tinder",                "category": "Subscriptions"},
    {"pattern": "TINDER ",            "match": "prefix",   "merchant": "Tinder",                "category": "Subscriptions"},
    {"pattern": "MATCH*TINDER",       "match": "prefix",   "merchant": "Tinder",                "category": "Subscriptions"},
    {"pattern": "MATCH GROUP",        "match": "prefix",   "merchant": "Match Group",           "category": "Subscriptions"},
    {"pattern": "BUMBLE.COM",         "match": "prefix",   "merchant": "Bumble",                "category": "Subscriptions"},
    {"pattern": "BUMBLE HOLDING",     "match": "prefix",   "merchant": "Bumble",                "category": "Subscriptions"},
    {"pattern": "BUMBLE ",            "match": "prefix",   "merchant": "Bumble",                "category": "Subscriptions"},
    {"pattern": "HINGE",              "match": "prefix",   "merchant": "Hinge",                 "category": "Subscriptions"},

    # ── DUTCH TELECOM ─────────────────────────────────────────────────────
    {"pattern": "KPN",                "match": "prefix",   "merchant": "KPN",                   "category": "Telecom"},
    {"pattern": "VODAFONE",           "match": "prefix",   "merchant": "Vodafone",              "category": "Telecom"},
    {"pattern": "T-MOBILE",           "match": "prefix",   "merchant": "T-Mobile",              "category": "Telecom"},
    {"pattern": "TMOBILE",            "match": "prefix",   "merchant": "T-Mobile",              "category": "Telecom"},
    {"pattern": "ZIGGO",              "match": "prefix",   "merchant": "Ziggo",                 "category": "Telecom"},
    {"pattern": "SIMYO",              "match": "prefix",   "merchant": "Simyo",                 "category": "Telecom"},
    {"pattern": "BEN NL",             "match": "prefix",   "merchant": "Ben",                   "category": "Telecom"},
    {"pattern": "LEBARA",             "match": "prefix",   "merchant": "Lebara",                "category": "Telecom"},
    {"pattern": "HOLLANDSNIEUWE",     "match": "prefix",   "merchant": "hollandsnieuwe",        "category": "Telecom"},

    # ── INSURANCE (NL) ────────────────────────────────────────────────────
    {"pattern": "CENTRAAL BEHEER",    "match": "prefix",   "merchant": "Centraal Beheer",       "category": "Insurance"},
    {"pattern": "NATIONALE NEDERLANDEN","match": "prefix", "merchant": "Nationale-Nederlanden", "category": "Insurance"},
    {"pattern": "NN GROUP",           "match": "prefix",   "merchant": "Nationale-Nederlanden", "category": "Insurance"},
    {"pattern": "ZILVEREN KRUIS",     "match": "prefix",   "merchant": "Zilveren Kruis",        "category": "Insurance"},
    {"pattern": "CZ GROEP",          "match": "prefix",   "merchant": "CZ",                    "category": "Insurance"},
    {"pattern": "MENZIS",             "match": "prefix",   "merchant": "Menzis",                "category": "Insurance"},
    {"pattern": "VGZ",                "match": "prefix",   "merchant": "VGZ",                   "category": "Insurance"},
    {"pattern": "OHRA",               "match": "prefix",   "merchant": "OHRA",                  "category": "Insurance"},
    {"pattern": "UNIVE",              "match": "prefix",   "merchant": "Unive",                 "category": "Insurance"},
    {"pattern": "AEGON",              "match": "prefix",   "merchant": "Aegon",                 "category": "Insurance"},

    # ── UTILITIES / ENERGY (NL) ───────────────────────────────────────────
    {"pattern": "VATTENFALL",         "match": "prefix",   "merchant": "Vattenfall",            "category": "Utilities"},
    {"pattern": "ENECO",              "match": "prefix",   "merchant": "Eneco",                 "category": "Utilities"},
    {"pattern": "ESSENT",             "match": "prefix",   "merchant": "Essent",                "category": "Utilities"},
    {"pattern": "GREENCHOICE",        "match": "prefix",   "merchant": "Greenchoice",           "category": "Utilities"},
    {"pattern": "BUDGET ENERGIE",     "match": "prefix",   "merchant": "Budget Energie",        "category": "Utilities"},
    {"pattern": "DUNEA",              "match": "prefix",   "merchant": "Dunea",                 "category": "Utilities"},
    {"pattern": "WATERNET",           "match": "prefix",   "merchant": "Waternet",              "category": "Utilities"},
    {"pattern": "PWN",                "match": "prefix",   "merchant": "PWN",                   "category": "Utilities"},
    {"pattern": "VITENS",             "match": "prefix",   "merchant": "Vitens",                "category": "Utilities"},

    # ── HEALTH & FITNESS ──────────────────────────────────────────────────
    {"pattern": "BASIC-FIT",          "match": "prefix",   "merchant": "Basic-Fit",             "category": "Sports & Fitness"},
    {"pattern": "BASICFIT",           "match": "prefix",   "merchant": "Basic-Fit",             "category": "Sports & Fitness"},
    {"pattern": "SPORTCITY",          "match": "prefix",   "merchant": "SportCity",             "category": "Sports & Fitness"},
    {"pattern": "ANYTIME FITNESS",    "match": "prefix",   "merchant": "Anytime Fitness",       "category": "Sports & Fitness"},
    {"pattern": "FIT FOR FREE",       "match": "prefix",   "merchant": "Fit For Free",          "category": "Sports & Fitness"},
    {"pattern": "TRAINMORE",          "match": "prefix",   "merchant": "TrainMore",             "category": "Sports & Fitness"},

    # ── DUTCH GOVERNMENT & TAXES ──────────────────────────────────────────
    {"pattern": "BELASTINGDIENST",    "match": "prefix",   "merchant": "Belastingdienst",       "category": "Government & Tax"},
    {"pattern": "GEMEENTE",           "match": "prefix",   "merchant": "Gemeente",              "category": "Government & Tax"},
    {"pattern": "RDW",                "match": "prefix",   "merchant": "RDW",                   "category": "Government & Tax"},
    {"pattern": "DUO ",               "match": "prefix",   "merchant": "DUO",                   "category": "Education"},
    {"pattern": "CJIB",               "match": "prefix",   "merchant": "CJIB",                  "category": "Government & Tax"},
    {"pattern": "SVB",                "match": "prefix",   "merchant": "SVB",                   "category": "Government & Tax"},
    {"pattern": "CAK",                "match": "prefix",   "merchant": "CAK",                   "category": "Government & Tax"},

    # ── FINANCIAL SERVICES ────────────────────────────────────────────────
    {"pattern": "TIKKIE",             "match": "prefix",   "merchant": "Tikkie",                "category": "Financial Services"},
    {"pattern": "IDEAL",              "match": "contains", "merchant": "iDEAL",                 "category": "Financial Services"},
    {"pattern": "WISE.COM",           "match": "prefix",   "merchant": "Wise",                  "category": "Financial Services"},
    {"pattern": "TRANSFERWISE",       "match": "prefix",   "merchant": "Wise",                  "category": "Financial Services"},
    {"pattern": "REVOLUT",            "match": "prefix",   "merchant": "Revolut",               "category": "Financial Services"},
    {"pattern": "STRIPE",             "match": "prefix",   "merchant": "Stripe",                "category": "Financial Services"},
    {"pattern": "MOLLIE",             "match": "prefix",   "merchant": "Mollie",                "category": "Financial Services"},
    {"pattern": "ADYEN",              "match": "prefix",   "merchant": "Adyen",                 "category": "Financial Services"},
    {"pattern": "KLARNA",             "match": "prefix",   "merchant": "Klarna",                "category": "Financial Services"},
    {"pattern": "AFTERPAY",           "match": "prefix",   "merchant": "Afterpay",              "category": "Financial Services"},
    {"pattern": "IN3 BV",             "match": "prefix",   "merchant": "in3",                   "category": "Financial Services"},

    # ── ING-SPECIFIC PATTERNS ─────────────────────────────────────────────
    {"pattern": r"^BETAALAUTOMAAT\s",  "match": "regex",   "merchant": "",                      "category": ""},
    {"pattern": "SEPA OVERBOEKING",   "match": "prefix",   "merchant": "",                      "category": ""},
    {"pattern": "SEPA INCASSO",       "match": "prefix",   "merchant": "",                      "category": ""},
]

# Total: ~300 patterns

# Precompile regex patterns for performance
_COMPILED: list[tuple[re.Pattern | str, str, dict]] = []
for p in MERCHANT_PATTERNS:
    match_type = p["match"]
    if match_type == "regex":
        _COMPILED.append((re.compile(p["pattern"], re.IGNORECASE), "regex", p))
    elif match_type == "prefix":
        _COMPILED.append((p["pattern"].upper(), "prefix", p))
    elif match_type == "contains":
        _COMPILED.append((p["pattern"].upper(), "contains", p))


def match_merchant(description: str) -> MerchantMatch | None:
    """Match a bank statement description to a known merchant.

    Returns MerchantMatch if found, None otherwise.
    Matching is case-insensitive. First match wins (patterns are ordered
    by specificity — more specific patterns come first).
    """
    if not description:
        return None

    desc_upper = description.upper().strip()

    for compiled_pattern, match_type, entry in _COMPILED:
        matched = False
        if match_type == "regex":
            matched = compiled_pattern.search(desc_upper) is not None
        elif match_type == "prefix":
            matched = desc_upper.startswith(compiled_pattern)
        elif match_type == "contains":
            matched = compiled_pattern in desc_upper

        if matched and entry["merchant"]:
            return MerchantMatch(
                merchant=entry["merchant"],
                category=entry["category"],
                original_description=description,
            )

    return None


def categorize_description(description: str) -> tuple[str, str]:
    """Convenience: returns (merchant, category) or ("", "") if no match."""
    result = match_merchant(description)
    if result:
        return result.merchant, result.category
    return "", ""


def get_all_categories() -> list[str]:
    """Return sorted list of all unique categories used in patterns."""
    return sorted({p["category"] for p in MERCHANT_PATTERNS if p["category"]})


def get_all_merchants() -> list[str]:
    """Return sorted list of all unique merchant names in patterns."""
    return sorted({p["merchant"] for p in MERCHANT_PATTERNS if p["merchant"]})


def get_patterns_by_category(category: str) -> list[dict]:
    """Return all patterns for a given category."""
    return [p for p in MERCHANT_PATTERNS if p["category"] == category]
