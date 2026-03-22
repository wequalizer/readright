#!/usr/bin/env python3
"""Generate source_catalog.json with 10,000+ data source entries for ReadRight SEO pages."""

from __future__ import annotations
import json
import re
from pathlib import Path

OUTPUT = Path(__file__).parent.parent / "api" / "data" / "source_catalog.json"

# ── Existing parsers (40) ────────────────────────────────────────────────
EXISTING_PARSERS = {
    "ing_csv_nl": ("ING Bank", "NL", "banking"),
    "rabobank_csv_nl": ("Rabobank", "NL", "banking"),
    "abn_amro_csv_nl": ("ABN AMRO", "NL", "banking"),
    "triodos_csv_nl": ("Triodos Bank", "NL", "banking"),
    "bunq_csv": ("Bunq", "NL", "banking"),
    "chase_csv_us": ("Chase Bank", "US", "banking"),
    "bofa_csv_us": ("Bank of America", "US", "banking"),
    "wellsfargo_csv_us": ("Wells Fargo", "US", "banking"),
    "citi_csv_us": ("Citibank", "US", "banking"),
    "barclays_csv_uk": ("Barclays", "GB", "banking"),
    "hsbc_csv": ("HSBC", "GB", "banking"),
    "monzo_csv_uk": ("Monzo", "GB", "banking"),
    "revolut_csv": ("Revolut", "EU", "banking"),
    "wise_csv": ("Wise", "EU", "banking"),
    "n26_csv": ("N26", "EU", "banking"),
    "bank_ofx": ("OFX/QFX Bank Export", "INTL", "banking"),
    "bank_qif": ("QIF Bank Export", "INTL", "banking"),
    "paypal_csv": ("PayPal", "INTL", "payments"),
    "venmo_csv": ("Venmo", "US", "payments"),
    "cashapp_csv": ("Cash App", "US", "payments"),
    "stripe_csv": ("Stripe", "INTL", "payments"),
    "square_csv": ("Square", "INTL", "payments"),
    "shopify_orders_csv": ("Shopify", "INTL", "ecommerce"),
    "ynab_csv": ("YNAB", "INTL", "finance"),
    "whatsapp_txt": ("WhatsApp", "INTL", "chat"),
    "telegram_json": ("Telegram", "INTL", "chat"),
    "discord_csv": ("Discord", "INTL", "chat"),
    "signal_txt": ("Signal", "INTL", "chat"),
    "facebook_messages_json": ("Facebook Messenger", "INTL", "social"),
    "instagram_messages_json": ("Instagram", "INTL", "social"),
    "twitter_archive_js": ("Twitter/X", "INTL", "social"),
    "linkedin_connections_csv": ("LinkedIn Connections", "INTL", "social"),
    "linkedin_messages_csv": ("LinkedIn Messages", "INTL", "social"),
    "google_contacts_csv": ("Google Contacts", "INTL", "contacts"),
    "google_takeout_activity_json": ("Google Takeout Activity", "INTL", "productivity"),
    "vcf_contacts": ("vCard Contacts", "INTL", "contacts"),
    "apple_health_xml": ("Apple Health", "INTL", "health"),
    "amazon_orders_csv": ("Amazon Orders", "INTL", "ecommerce"),
    "csv_generic": ("Generic CSV", "INTL", "generic"),
    "excel_generic": ("Generic Excel", "INTL", "generic"),
}

# ── Country data ─────────────────────────────────────────────────────────
# (code, name, flag, date_fmt, decimal, thousand, currency, curr_symbol, language)
COUNTRIES = {
    "US": ("United States", "🇺🇸", "MM/DD/YYYY", ".", ",", "USD", "$", "English"),
    "GB": ("United Kingdom", "🇬🇧", "DD/MM/YYYY", ".", ",", "GBP", "£", "English"),
    "NL": ("Netherlands", "🇳🇱", "DD-MM-YYYY", ",", ".", "EUR", "€", "Dutch"),
    "DE": ("Germany", "🇩🇪", "DD.MM.YYYY", ",", ".", "EUR", "€", "German"),
    "FR": ("France", "🇫🇷", "DD/MM/YYYY", ",", " ", "EUR", "€", "French"),
    "ES": ("Spain", "🇪🇸", "DD/MM/YYYY", ",", ".", "EUR", "€", "Spanish"),
    "IT": ("Italy", "🇮🇹", "DD/MM/YYYY", ",", ".", "EUR", "€", "Italian"),
    "PT": ("Portugal", "🇵🇹", "DD/MM/YYYY", ",", ".", "EUR", "€", "Portuguese"),
    "BE": ("Belgium", "🇧🇪", "DD/MM/YYYY", ",", ".", "EUR", "€", "Dutch/French"),
    "AT": ("Austria", "🇦🇹", "DD.MM.YYYY", ",", ".", "EUR", "€", "German"),
    "CH": ("Switzerland", "🇨🇭", "DD.MM.YYYY", ".", "'", "CHF", "CHF", "German/French"),
    "SE": ("Sweden", "🇸🇪", "YYYY-MM-DD", ",", " ", "SEK", "kr", "Swedish"),
    "NO": ("Norway", "🇳🇴", "DD.MM.YYYY", ",", " ", "NOK", "kr", "Norwegian"),
    "DK": ("Denmark", "🇩🇰", "DD-MM-YYYY", ",", ".", "DKK", "kr", "Danish"),
    "FI": ("Finland", "🇫🇮", "DD.MM.YYYY", ",", " ", "EUR", "€", "Finnish"),
    "IE": ("Ireland", "🇮🇪", "DD/MM/YYYY", ".", ",", "EUR", "€", "English"),
    "PL": ("Poland", "🇵🇱", "DD.MM.YYYY", ",", " ", "PLN", "zł", "Polish"),
    "CZ": ("Czech Republic", "🇨🇿", "DD.MM.YYYY", ",", " ", "CZK", "Kč", "Czech"),
    "SK": ("Slovakia", "🇸🇰", "DD.MM.YYYY", ",", " ", "EUR", "€", "Slovak"),
    "HU": ("Hungary", "🇭🇺", "YYYY.MM.DD", ",", " ", "HUF", "Ft", "Hungarian"),
    "RO": ("Romania", "🇷🇴", "DD.MM.YYYY", ",", ".", "RON", "lei", "Romanian"),
    "BG": ("Bulgaria", "🇧🇬", "DD.MM.YYYY", ",", " ", "BGN", "лв", "Bulgarian"),
    "HR": ("Croatia", "🇭🇷", "DD.MM.YYYY", ",", ".", "EUR", "€", "Croatian"),
    "SI": ("Slovenia", "🇸🇮", "DD.MM.YYYY", ",", ".", "EUR", "€", "Slovenian"),
    "RS": ("Serbia", "🇷🇸", "DD.MM.YYYY", ",", ".", "RSD", "din", "Serbian"),
    "BA": ("Bosnia", "🇧🇦", "DD.MM.YYYY", ",", ".", "BAM", "KM", "Bosnian"),
    "ME": ("Montenegro", "🇲🇪", "DD.MM.YYYY", ",", ".", "EUR", "€", "Montenegrin"),
    "MK": ("North Macedonia", "🇲🇰", "DD.MM.YYYY", ",", ".", "MKD", "ден", "Macedonian"),
    "AL": ("Albania", "🇦🇱", "DD.MM.YYYY", ",", " ", "ALL", "L", "Albanian"),
    "GR": ("Greece", "🇬🇷", "DD/MM/YYYY", ",", ".", "EUR", "€", "Greek"),
    "CY": ("Cyprus", "🇨🇾", "DD/MM/YYYY", ",", ".", "EUR", "€", "Greek"),
    "MT": ("Malta", "🇲🇹", "DD/MM/YYYY", ".", ",", "EUR", "€", "English"),
    "LU": ("Luxembourg", "🇱🇺", "DD/MM/YYYY", ",", ".", "EUR", "€", "French/German"),
    "EE": ("Estonia", "🇪🇪", "DD.MM.YYYY", ",", " ", "EUR", "€", "Estonian"),
    "LV": ("Latvia", "🇱🇻", "DD.MM.YYYY", ",", " ", "EUR", "€", "Latvian"),
    "LT": ("Lithuania", "🇱🇹", "YYYY-MM-DD", ",", " ", "EUR", "€", "Lithuanian"),
    "UA": ("Ukraine", "🇺🇦", "DD.MM.YYYY", ",", " ", "UAH", "₴", "Ukrainian"),
    "RU": ("Russia", "🇷🇺", "DD.MM.YYYY", ",", " ", "RUB", "₽", "Russian"),
    "TR": ("Turkey", "🇹🇷", "DD.MM.YYYY", ",", ".", "TRY", "₺", "Turkish"),
    "IL": ("Israel", "🇮🇱", "DD/MM/YYYY", ".", ",", "ILS", "₪", "Hebrew"),
    "SA": ("Saudi Arabia", "🇸🇦", "DD/MM/YYYY", ".", ",", "SAR", "﷼", "Arabic"),
    "AE": ("UAE", "🇦🇪", "DD/MM/YYYY", ".", ",", "AED", "د.إ", "Arabic/English"),
    "QA": ("Qatar", "🇶🇦", "DD/MM/YYYY", ".", ",", "QAR", "﷼", "Arabic"),
    "KW": ("Kuwait", "🇰🇼", "DD/MM/YYYY", ".", ",", "KWD", "د.ك", "Arabic"),
    "BH": ("Bahrain", "🇧🇭", "DD/MM/YYYY", ".", ",", "BHD", "BD", "Arabic"),
    "OM": ("Oman", "🇴🇲", "DD/MM/YYYY", ".", ",", "OMR", "﷼", "Arabic"),
    "JO": ("Jordan", "🇯🇴", "DD/MM/YYYY", ".", ",", "JOD", "JD", "Arabic"),
    "LB": ("Lebanon", "🇱🇧", "DD/MM/YYYY", ".", ",", "LBP", "ل.ل", "Arabic/French"),
    "EG": ("Egypt", "🇪🇬", "DD/MM/YYYY", ".", ",", "EGP", "E£", "Arabic"),
    "MA": ("Morocco", "🇲🇦", "DD/MM/YYYY", ",", ".", "MAD", "MAD", "Arabic/French"),
    "TN": ("Tunisia", "🇹🇳", "DD/MM/YYYY", ",", " ", "TND", "DT", "Arabic/French"),
    "DZ": ("Algeria", "🇩🇿", "DD/MM/YYYY", ",", " ", "DZD", "DA", "Arabic/French"),
    "NG": ("Nigeria", "🇳🇬", "DD/MM/YYYY", ".", ",", "NGN", "₦", "English"),
    "GH": ("Ghana", "🇬🇭", "DD/MM/YYYY", ".", ",", "GHS", "GH₵", "English"),
    "KE": ("Kenya", "🇰🇪", "DD/MM/YYYY", ".", ",", "KES", "KSh", "English/Swahili"),
    "ZA": ("South Africa", "🇿🇦", "YYYY/MM/DD", ".", ",", "ZAR", "R", "English"),
    "TZ": ("Tanzania", "🇹🇿", "DD/MM/YYYY", ".", ",", "TZS", "TSh", "Swahili/English"),
    "UG": ("Uganda", "🇺🇬", "DD/MM/YYYY", ".", ",", "UGX", "USh", "English"),
    "ET": ("Ethiopia", "🇪🇹", "DD/MM/YYYY", ".", ",", "ETB", "Br", "Amharic"),
    "RW": ("Rwanda", "🇷🇼", "DD/MM/YYYY", ".", ",", "RWF", "RF", "English/French"),
    "SN": ("Senegal", "🇸🇳", "DD/MM/YYYY", ",", " ", "XOF", "CFA", "French"),
    "CI": ("Côte d'Ivoire", "🇨🇮", "DD/MM/YYYY", ",", " ", "XOF", "CFA", "French"),
    "CM": ("Cameroon", "🇨🇲", "DD/MM/YYYY", ",", " ", "XAF", "CFA", "French/English"),
    "IN": ("India", "🇮🇳", "DD/MM/YYYY", ".", ",", "INR", "₹", "English/Hindi"),
    "PK": ("Pakistan", "🇵🇰", "DD/MM/YYYY", ".", ",", "PKR", "₨", "English/Urdu"),
    "BD": ("Bangladesh", "🇧🇩", "DD/MM/YYYY", ".", ",", "BDT", "৳", "Bengali"),
    "LK": ("Sri Lanka", "🇱🇰", "DD/MM/YYYY", ".", ",", "LKR", "Rs", "Sinhala/Tamil"),
    "NP": ("Nepal", "🇳🇵", "DD/MM/YYYY", ".", ",", "NPR", "Rs", "Nepali"),
    "MM": ("Myanmar", "🇲🇲", "DD/MM/YYYY", ".", ",", "MMK", "K", "Burmese"),
    "TH": ("Thailand", "🇹🇭", "DD/MM/YYYY", ".", ",", "THB", "฿", "Thai"),
    "VN": ("Vietnam", "🇻🇳", "DD/MM/YYYY", ",", ".", "VND", "₫", "Vietnamese"),
    "ID": ("Indonesia", "🇮🇩", "DD/MM/YYYY", ",", ".", "IDR", "Rp", "Indonesian"),
    "MY": ("Malaysia", "🇲🇾", "DD/MM/YYYY", ".", ",", "MYR", "RM", "Malay/English"),
    "SG": ("Singapore", "🇸🇬", "DD/MM/YYYY", ".", ",", "SGD", "S$", "English"),
    "PH": ("Philippines", "🇵🇭", "MM/DD/YYYY", ".", ",", "PHP", "₱", "English/Filipino"),
    "KH": ("Cambodia", "🇰🇭", "DD/MM/YYYY", ".", ",", "KHR", "៛", "Khmer"),
    "CN": ("China", "🇨🇳", "YYYY-MM-DD", ".", ",", "CNY", "¥", "Chinese"),
    "JP": ("Japan", "🇯🇵", "YYYY/MM/DD", ".", ",", "JPY", "¥", "Japanese"),
    "KR": ("South Korea", "🇰🇷", "YYYY-MM-DD", ".", ",", "KRW", "₩", "Korean"),
    "TW": ("Taiwan", "🇹🇼", "YYYY/MM/DD", ".", ",", "TWD", "NT$", "Chinese"),
    "HK": ("Hong Kong", "🇭🇰", "DD/MM/YYYY", ".", ",", "HKD", "HK$", "Chinese/English"),
    "MO": ("Macau", "🇲🇴", "DD/MM/YYYY", ".", ",", "MOP", "MOP$", "Chinese/Portuguese"),
    "AU": ("Australia", "🇦🇺", "DD/MM/YYYY", ".", ",", "AUD", "A$", "English"),
    "NZ": ("New Zealand", "🇳🇿", "DD/MM/YYYY", ".", ",", "NZD", "NZ$", "English"),
    "CA": ("Canada", "🇨🇦", "YYYY-MM-DD", ".", ",", "CAD", "C$", "English/French"),
    "MX": ("Mexico", "🇲🇽", "DD/MM/YYYY", ".", ",", "MXN", "$", "Spanish"),
    "BR": ("Brazil", "🇧🇷", "DD/MM/YYYY", ",", ".", "BRL", "R$", "Portuguese"),
    "AR": ("Argentina", "🇦🇷", "DD/MM/YYYY", ",", ".", "ARS", "$", "Spanish"),
    "CL": ("Chile", "🇨🇱", "DD-MM-YYYY", ",", ".", "CLP", "$", "Spanish"),
    "CO": ("Colombia", "🇨🇴", "DD/MM/YYYY", ",", ".", "COP", "$", "Spanish"),
    "PE": ("Peru", "🇵🇪", "DD/MM/YYYY", ".", ",", "PEN", "S/", "Spanish"),
    "EC": ("Ecuador", "🇪🇨", "DD/MM/YYYY", ",", ".", "USD", "$", "Spanish"),
    "VE": ("Venezuela", "🇻🇪", "DD/MM/YYYY", ",", ".", "VES", "Bs", "Spanish"),
    "UY": ("Uruguay", "🇺🇾", "DD/MM/YYYY", ",", ".", "UYU", "$U", "Spanish"),
    "PY": ("Paraguay", "🇵🇾", "DD/MM/YYYY", ",", ".", "PYG", "₲", "Spanish"),
    "BO": ("Bolivia", "🇧🇴", "DD/MM/YYYY", ",", ".", "BOB", "Bs", "Spanish"),
    "CR": ("Costa Rica", "🇨🇷", "DD/MM/YYYY", ",", ".", "CRC", "₡", "Spanish"),
    "PA": ("Panama", "🇵🇦", "DD/MM/YYYY", ".", ",", "USD", "$", "Spanish"),
    "DO": ("Dominican Republic", "🇩🇴", "DD/MM/YYYY", ".", ",", "DOP", "RD$", "Spanish"),
    "GT": ("Guatemala", "🇬🇹", "DD/MM/YYYY", ".", ",", "GTQ", "Q", "Spanish"),
    "KZ": ("Kazakhstan", "🇰🇿", "DD.MM.YYYY", ",", " ", "KZT", "₸", "Kazakh/Russian"),
    "UZ": ("Uzbekistan", "🇺🇿", "DD.MM.YYYY", ",", " ", "UZS", "сўм", "Uzbek"),
    "GE": ("Georgia", "🇬🇪", "DD.MM.YYYY", ",", " ", "GEL", "₾", "Georgian"),
    "AM": ("Armenia", "🇦🇲", "DD.MM.YYYY", ",", " ", "AMD", "֏", "Armenian"),
    "AZ": ("Azerbaijan", "🇦🇿", "DD.MM.YYYY", ",", " ", "AZN", "₼", "Azerbaijani"),
    "MN": ("Mongolia", "🇲🇳", "YYYY.MM.DD", ",", " ", "MNT", "₮", "Mongolian"),
    "IS": ("Iceland", "🇮🇸", "DD.MM.YYYY", ",", ".", "ISK", "kr", "Icelandic"),
}

# ── Bank data per country ────────────────────────────────────────────────
BANKS: dict[str, list[tuple[str, str]]] = {
    "US": [
        ("JPMorgan Chase", "82M+"), ("Bank of America", "69M+"), ("Wells Fargo", "70M+"),
        ("Citibank", "200M+ global"), ("Capital One", "50M+"), ("U.S. Bank", "19M+"),
        ("PNC Financial", "12M+"), ("Truist", "15M+"), ("TD Bank US", "10M+"),
        ("Ally Bank", "3M+"), ("Discover Bank", "5M+"), ("Goldman Sachs Marcus", "2M+"),
        ("Charles Schwab", "35M+"), ("Fidelity", "43M+"), ("USAA", "13M+"),
        ("Navy Federal", "13M+"), ("SoFi", "8M+"), ("Chime", "22M+"),
        ("Wealthfront", "1M+"), ("Betterment", "900K+"),
    ],
    "GB": [
        ("Barclays", "24M+"), ("HSBC UK", "15M+"), ("Lloyds Bank", "26M+"),
        ("NatWest", "19M+"), ("Santander UK", "14M+"), ("Nationwide", "16M+"),
        ("TSB", "5M+"), ("Metro Bank", "3M+"), ("Starling Bank", "4M+"),
        ("Monzo", "9M+"), ("Virgin Money", "6M+"), ("Co-operative Bank", "3M+"),
        ("Halifax", "22M+"), ("RBS", "7M+"), ("Revolut UK", "10M+"),
    ],
    "NL": [
        ("ING Bank", "14M+"), ("Rabobank", "9M+"), ("ABN AMRO", "5M+"),
        ("Triodos Bank", "800K+"), ("Bunq", "1M+"), ("ASN Bank", "800K+"),
        ("Knab", "300K+"), ("SNS", "1M+"), ("RegioBank", "500K+"),
        ("Van Lanschot", "200K+"),
    ],
    "DE": [
        ("Deutsche Bank", "19M+"), ("Commerzbank", "11M+"), ("DKB", "5M+"),
        ("ING Germany", "9M+"), ("Sparkasse", "50M+"), ("N26", "8M+"),
        ("Comdirect", "3M+"), ("Consorsbank", "1.5M+"), ("GLS Bank", "300K+"),
        ("Norisbank", "1M+"), ("Postbank", "12M+"), ("Targobank", "4M+"),
        ("HypoVereinsbank", "3M+"), ("Volksbank", "30M+"),
    ],
    "FR": [
        ("BNP Paribas", "31M+"), ("Société Générale", "26M+"), ("Crédit Agricole", "52M+"),
        ("Crédit Mutuel", "30M+"), ("Banque Populaire", "9M+"), ("La Banque Postale", "11M+"),
        ("CIC", "5M+"), ("LCL", "6M+"), ("Boursorama", "5M+"),
        ("Fortuneo", "1M+"), ("Hello Bank France", "500K+"), ("Orange Bank", "2M+"),
    ],
    "ES": [
        ("Santander Spain", "15M+"), ("BBVA Spain", "20M+"), ("CaixaBank", "21M+"),
        ("Bankinter", "2M+"), ("Sabadell", "11M+"), ("ING Spain", "4M+"),
        ("Openbank", "2M+"), ("Abanca", "3M+"), ("Unicaja", "5M+"),
        ("Kutxabank", "3M+"),
    ],
    "IT": [
        ("Intesa Sanpaolo", "12M+"), ("UniCredit Italy", "8M+"), ("Banco BPM", "4M+"),
        ("Monte dei Paschi", "3M+"), ("BPER Banca", "5M+"), ("FinecoBank", "1.5M+"),
        ("Mediolanum", "2M+"), ("ING Italy", "1M+"), ("N26 Italy", "1M+"),
        ("Widiba", "300K+"),
    ],
    "PT": [
        ("Caixa Geral", "5M+"), ("Millennium BCP", "3M+"), ("Novo Banco", "2M+"),
        ("Santander Portugal", "2M+"), ("BPI", "2M+"), ("ActivoBank", "500K+"),
        ("Bankinter Portugal", "200K+"), ("Moey", "300K+"),
    ],
    "IN": [
        ("HDFC Bank", "93M+"), ("State Bank of India", "500M+"), ("ICICI Bank", "55M+"),
        ("Axis Bank", "30M+"), ("Kotak Mahindra", "40M+"), ("Punjab National Bank", "18M+"),
        ("Bank of Baroda", "15M+"), ("Canara Bank", "12M+"), ("Union Bank", "12M+"),
        ("IndusInd Bank", "10M+"), ("Yes Bank", "5M+"), ("IDFC First", "8M+"),
        ("Federal Bank", "3M+"), ("RBL Bank", "2M+"), ("Bandhan Bank", "3M+"),
        ("Bank of India", "10M+"), ("Indian Bank", "8M+"), ("Central Bank", "5M+"),
    ],
    "BR": [
        ("Nubank", "100M+"), ("Banco do Brasil", "78M+"), ("Itaú Unibanco", "60M+"),
        ("Bradesco", "72M+"), ("Santander Brazil", "62M+"), ("Caixa Econômica", "145M+"),
        ("BTG Pactual", "10M+"), ("Banco Inter", "30M+"), ("C6 Bank", "30M+"),
        ("Neon", "20M+"), ("PagBank", "30M+"), ("Banco Original", "5M+"),
    ],
    "JP": [
        ("MUFG Bank", "40M+"), ("SMBC", "30M+"), ("Mizuho Bank", "25M+"),
        ("Resona Bank", "15M+"), ("Japan Post Bank", "120M+"), ("Rakuten Bank", "14M+"),
        ("SBI Sumishin", "6M+"), ("Sony Bank", "2M+"), ("Aeon Bank", "5M+"),
        ("Seven Bank", "3M+"),
    ],
    "AU": [
        ("Commonwealth Bank", "17M+"), ("Westpac", "14M+"), ("ANZ", "9M+"),
        ("NAB", "9M+"), ("Macquarie Bank", "2M+"), ("ING Australia", "2M+"),
        ("Bendigo Bank", "2M+"), ("Suncorp", "3M+"), ("Bankwest", "1M+"),
        ("Up Bank", "800K+"),
    ],
    "CA": [
        ("Royal Bank of Canada", "17M+"), ("TD Canada Trust", "16M+"), ("BMO", "13M+"),
        ("Scotiabank", "25M+"), ("CIBC", "11M+"), ("National Bank", "3M+"),
        ("Tangerine", "2M+"), ("Simplii Financial", "2M+"), ("EQ Bank", "500K+"),
        ("Desjardins", "7M+"), ("ATB Financial", "800K+"), ("Wealthsimple", "3M+"),
    ],
    "CN": [
        ("ICBC", "700M+"), ("CCB", "600M+"), ("Agricultural Bank", "500M+"),
        ("Bank of China", "400M+"), ("BoCom", "200M+"), ("China Merchants Bank", "180M+"),
        ("CITIC Bank", "100M+"), ("Minsheng Bank", "80M+"), ("Everbright Bank", "50M+"),
        ("Ping An Bank", "120M+"), ("WeBank", "350M+"), ("MYbank", "45M+"),
    ],
    "KR": [
        ("KB Kookmin", "30M+"), ("Shinhan Bank", "25M+"), ("Hana Bank", "20M+"),
        ("Woori Bank", "15M+"), ("NH NongHyup", "10M+"), ("IBK", "15M+"),
        ("KEB Hana", "5M+"), ("Toss Bank", "8M+"), ("Kakao Bank", "22M+"),
        ("K Bank", "10M+"),
    ],
    "MX": [
        ("BBVA Mexico", "25M+"), ("Banorte", "20M+"), ("Citibanamex", "20M+"),
        ("Santander Mexico", "15M+"), ("HSBC Mexico", "5M+"), ("Scotiabank Mexico", "3M+"),
        ("Banco Azteca", "50M+"), ("BanCoppel", "15M+"), ("Nu Mexico", "5M+"),
        ("Hey Banco", "1M+"),
    ],
    "SG": [
        ("DBS", "5M+"), ("OCBC", "4M+"), ("UOB", "4M+"),
        ("Standard Chartered SG", "2M+"), ("HSBC Singapore", "1M+"), ("Citibank SG", "1M+"),
        ("Maybank SG", "1M+"), ("GXS Bank", "500K+"), ("Trust Bank", "800K+"),
    ],
    "TR": [
        ("Ziraat Bankası", "25M+"), ("İş Bankası", "20M+"), ("Garanti BBVA", "18M+"),
        ("Akbank", "15M+"), ("Yapı Kredi", "12M+"), ("Halkbank", "10M+"),
        ("VakıfBank", "10M+"), ("QNB Finansbank", "5M+"), ("Denizbank", "5M+"),
        ("TEB", "3M+"), ("Enpara", "2M+"),
    ],
    "PL": [
        ("PKO BP", "11M+"), ("Bank Pekao", "6M+"), ("mBank", "6M+"),
        ("ING Poland", "4M+"), ("Santander Poland", "7M+"), ("BNP Paribas Poland", "4M+"),
        ("Millennium Bank", "3M+"), ("Alior Bank", "4M+"), ("Credit Agricole Poland", "2M+"),
    ],
    "SE": [
        ("Swedbank", "8M+"), ("SEB", "4M+"), ("Handelsbanken", "3M+"),
        ("Nordea Sweden", "4M+"), ("Danske Bank Sweden", "1M+"), ("Skandia", "1M+"),
        ("Avanza", "2M+"), ("Länsförsäkringar", "4M+"), ("SBAB", "500K+"),
        ("Klarna Bank", "5M+"),
    ],
    "CH": [
        ("UBS", "4M+"), ("Credit Suisse", "2M+"), ("Raiffeisen Switzerland", "4M+"),
        ("PostFinance", "3M+"), ("ZKB", "2M+"), ("Migros Bank", "1M+"),
        ("Julius Bär", "500K+"), ("Neon Switzerland", "200K+"), ("Yuh", "300K+"),
    ],
    "AE": [
        ("Emirates NBD", "5M+"), ("ADCB", "3M+"), ("First Abu Dhabi", "4M+"),
        ("Mashreq Bank", "2M+"), ("RAKBANK", "2M+"), ("CBD", "1M+"),
        ("Dubai Islamic Bank", "3M+"), ("Al Hilal Bank", "1M+"), ("Liv", "500K+"),
        ("Wio Bank", "300K+"),
    ],
    "NG": [
        ("GTBank", "30M+"), ("Access Bank", "50M+"), ("Zenith Bank", "25M+"),
        ("First Bank", "30M+"), ("UBA", "35M+"), ("Stanbic IBTC", "5M+"),
        ("Fidelity Bank", "10M+"), ("Sterling Bank", "5M+"), ("ALAT by Wema", "3M+"),
        ("OPay", "35M+"), ("Kuda Bank", "6M+"), ("PalmPay", "25M+"),
    ],
    "ZA": [
        ("Standard Bank", "12M+"), ("FNB", "10M+"), ("ABSA", "12M+"),
        ("Nedbank", "8M+"), ("Capitec", "20M+"), ("Discovery Bank", "2M+"),
        ("TymeBank", "8M+"), ("African Bank", "3M+"), ("Investec", "500K+"),
    ],
    "ID": [
        ("BCA", "35M+"), ("BRI", "130M+"), ("BNI", "25M+"),
        ("Bank Mandiri", "40M+"), ("CIMB Niaga", "10M+"), ("Bank Danamon", "5M+"),
        ("Bank Permata", "3M+"), ("BTPN/Jenius", "5M+"), ("Bank Jago", "8M+"),
        ("Allo Bank", "5M+"), ("SeaBank", "10M+"),
    ],
    "TH": [
        ("Bangkok Bank", "10M+"), ("SCB", "15M+"), ("Kasikornbank", "20M+"),
        ("Krungthai Bank", "15M+"), ("TMBThanachart", "10M+"), ("Bank of Ayudhya", "5M+"),
        ("CIMB Thai", "3M+"), ("KKP", "2M+"),
    ],
    "MY": [
        ("Maybank", "22M+"), ("CIMB", "16M+"), ("Public Bank", "10M+"),
        ("RHB Bank", "5M+"), ("Hong Leong Bank", "4M+"), ("AmBank", "3M+"),
        ("Bank Islam", "4M+"), ("GXBank", "2M+"), ("Touch 'n Go", "20M+"),
    ],
    "PH": [
        ("BDO Unibank", "15M+"), ("BPI", "10M+"), ("Metrobank", "10M+"),
        ("UnionBank", "5M+"), ("RCBC", "3M+"), ("Security Bank", "3M+"),
        ("Landbank", "20M+"), ("PNB", "5M+"), ("GCash", "80M+"), ("Maya", "50M+"),
        ("Tonik", "2M+"),
    ],
    "VN": [
        ("Vietcombank", "30M+"), ("BIDV", "25M+"), ("VietinBank", "20M+"),
        ("Techcombank", "12M+"), ("VPBank", "15M+"), ("MB Bank", "20M+"),
        ("ACB", "5M+"), ("TPBank", "10M+"), ("Sacombank", "8M+"),
    ],
    "EG": [
        ("CIB Egypt", "3M+"), ("National Bank of Egypt", "15M+"), ("Banque Misr", "10M+"),
        ("QNB ALAHLI", "3M+"), ("AAIB", "2M+"), ("HSBC Egypt", "1M+"),
    ],
    "KE": [
        ("Equity Bank", "15M+"), ("KCB Group", "10M+"), ("Co-operative Bank", "8M+"),
        ("Stanbic Kenya", "2M+"), ("ABSA Kenya", "3M+"), ("NCBA", "5M+"),
        ("I&M Bank", "1M+"), ("M-Pesa", "50M+"),
    ],
    "PK": [
        ("Habib Bank", "25M+"), ("United Bank", "15M+"), ("MCB Bank", "10M+"),
        ("Allied Bank", "8M+"), ("Meezan Bank", "12M+"), ("Bank Alfalah", "8M+"),
        ("Faysal Bank", "5M+"), ("JazzCash", "40M+"), ("Easypaisa", "30M+"),
    ],
    "BD": [
        ("Islami Bank BD", "15M+"), ("Dutch-Bangla Bank", "10M+"), ("BRAC Bank", "5M+"),
        ("City Bank", "3M+"), ("EBL", "3M+"), ("Prime Bank", "2M+"), ("bKash", "70M+"),
    ],
    "AR": [
        ("Banco Nación", "10M+"), ("Banco Galicia", "8M+"), ("BBVA Argentina", "5M+"),
        ("Santander Argentina", "5M+"), ("Banco Macro", "4M+"), ("HSBC Argentina", "2M+"),
        ("Brubank", "5M+"), ("Ualá", "8M+"), ("Mercado Pago", "50M+"),
    ],
    "CO": [
        ("Bancolombia", "18M+"), ("Banco de Bogotá", "10M+"), ("Davivienda", "12M+"),
        ("BBVA Colombia", "5M+"), ("Nequi", "18M+"), ("Nu Colombia", "5M+"),
    ],
    "CL": [
        ("Banco de Chile", "5M+"), ("BancoEstado", "14M+"), ("Santander Chile", "4M+"),
        ("BCI", "3M+"), ("Scotiabank Chile", "2M+"), ("Itaú Chile", "1M+"),
        ("Mach", "3M+"), ("Tenpo", "2M+"),
    ],
    "PE": [
        ("BCP", "12M+"), ("BBVA Peru", "5M+"), ("Interbank", "5M+"),
        ("Scotiabank Peru", "3M+"), ("Yape", "15M+"),
    ],
    "RO": [
        ("Banca Transilvania", "5M+"), ("BCR", "4M+"), ("BRD", "3M+"),
        ("ING Romania", "2M+"), ("Raiffeisen Romania", "2M+"), ("CEC Bank", "3M+"),
    ],
    "HU": [
        ("OTP Bank", "5M+"), ("K&H Bank", "2M+"), ("Erste Hungary", "1M+"),
        ("Raiffeisen Hungary", "1M+"), ("MBH Bank", "2M+"), ("Revolut Hungary", "1M+"),
    ],
    "CZ": [
        ("Česká spořitelna", "5M+"), ("ČSOB", "4M+"), ("Komerční banka", "2M+"),
        ("Moneta", "1M+"), ("Raiffeisenbank CZ", "1M+"), ("Air Bank", "1M+"),
    ],
    "GR": [
        ("National Bank of Greece", "5M+"), ("Piraeus Bank", "6M+"), ("Eurobank", "3M+"),
        ("Alpha Bank", "4M+"),
    ],
    "DK": [
        ("Danske Bank", "3M+"), ("Nordea Denmark", "2M+"), ("Jyske Bank", "1M+"),
        ("Nykredit", "1M+"), ("Sydbank", "500K+"), ("Lunar", "500K+"),
    ],
    "NO": [
        ("DNB", "4M+"), ("Nordea Norway", "1M+"), ("SpareBank 1", "2M+"),
        ("Sbanken", "500K+"), ("Bulder Bank", "200K+"),
    ],
    "FI": [
        ("OP Financial", "4M+"), ("Nordea Finland", "3M+"), ("Danske Bank Finland", "1M+"),
        ("S-Pankki", "3M+"), ("Aktia", "500K+"),
    ],
    "IE": [
        ("AIB", "4M+"), ("Bank of Ireland", "3M+"), ("Permanent TSB", "1M+"),
        ("KBC Ireland", "500K+"), ("N26 Ireland", "200K+"), ("Revolut Ireland", "2M+"),
    ],
    "BE": [
        ("KBC", "4M+"), ("BNP Paribas Fortis", "3M+"), ("ING Belgium", "3M+"),
        ("Belfius", "4M+"), ("Argenta", "2M+"), ("Crelan", "1M+"),
    ],
    "AT": [
        ("Erste Bank Austria", "4M+"), ("Raiffeisen Austria", "3M+"), ("Bank Austria", "2M+"),
        ("BAWAG", "2M+"), ("Easybank", "500K+"), ("N26 Austria", "500K+"),
    ],
    "RU": [
        ("Sberbank", "100M+"), ("VTB", "30M+"), ("Alfa-Bank", "20M+"),
        ("Tinkoff", "30M+"), ("Gazprombank", "5M+"), ("Raiffeisen Russia", "3M+"),
        ("Rosbank", "5M+"),
    ],
    "UA": [
        ("PrivatBank", "25M+"), ("Monobank", "8M+"), ("Oschadbank", "20M+"),
        ("Raiffeisen Ukraine", "3M+"), ("PUMB", "3M+"),
    ],
    "SA": [
        ("Al Rajhi Bank", "12M+"), ("SNB", "6M+"), ("Riyad Bank", "5M+"),
        ("STC Pay", "10M+"), ("Banque Saudi Fransi", "2M+"), ("Alinma Bank", "3M+"),
    ],
    "IL": [
        ("Bank Hapoalim", "3M+"), ("Bank Leumi", "3M+"), ("Discount Bank", "2M+"),
        ("Mizrahi Tefahot", "2M+"), ("First International", "1M+"),
    ],
    "KZ": [
        ("Halyk Bank", "12M+"), ("Kaspi Bank", "15M+"), ("Forte Bank", "3M+"),
        ("Bank CenterCredit", "2M+"),
    ],
    "GE": [
        ("TBC Bank", "3M+"), ("Bank of Georgia", "2M+"), ("Liberty Bank", "1M+"),
    ],
    "IS": [("Landsbankinn", "200K+"), ("Íslandsbanki", "200K+"), ("Arion Bank", "200K+")],
    "LU": [("Spuerkeess", "300K+"), ("BGL BNP Paribas", "200K+"), ("ING Luxembourg", "100K+")],
    "MT": [("Bank of Valletta", "300K+"), ("HSBC Malta", "100K+"), ("APS Bank", "80K+")],
    "EE": [("Swedbank Estonia", "1M+"), ("LHV", "500K+"), ("SEB Estonia", "400K+")],
    "LV": [("Swedbank Latvia", "800K+"), ("SEB Latvia", "500K+"), ("Citadele", "400K+")],
    "LT": [("Swedbank Lithuania", "1.5M+"), ("SEB Lithuania", "1M+"), ("Luminor", "500K+"), ("Revolut Lithuania", "2M+")],
    "NZ": [("ANZ NZ", "2M+"), ("Westpac NZ", "1.5M+"), ("ASB", "1.5M+"), ("BNZ", "1M+"), ("Kiwibank", "1M+")],
    "QA": [("QNB", "3M+"), ("Commercial Bank Qatar", "1M+"), ("Doha Bank", "500K+")],
    "KW": [("National Bank of Kuwait", "2M+"), ("Kuwait Finance House", "2M+"), ("Burgan Bank", "500K+")],
    "BH": [("Bank of Bahrain", "500K+"), ("Ahli United", "500K+"), ("NBB", "300K+")],
    "OM": [("Bank Muscat", "2M+"), ("NBO", "500K+"), ("Sohar International", "300K+")],
    "JO": [("Arab Bank", "3M+"), ("Housing Bank Jordan", "2M+"), ("Jordan Ahli Bank", "1M+")],
    "MA": [("Attijariwafa Bank", "10M+"), ("BMCE", "5M+"), ("Banque Populaire Morocco", "8M+"), ("CIH Bank", "2M+")],
    "TN": [("BIAT", "2M+"), ("STB", "1M+"), ("BNA Tunisia", "1M+")],
    "GH": [("GCB Bank", "3M+"), ("Ecobank Ghana", "2M+"), ("Stanbic Ghana", "1M+"), ("MTN MoMo", "20M+")],
    "TZ": [("CRDB", "3M+"), ("NMB Tanzania", "5M+"), ("Stanbic Tanzania", "1M+"), ("M-Pesa Tanzania", "20M+")],
    "UG": [("Stanbic Uganda", "1M+"), ("dfcu Bank", "1M+"), ("Centenary Bank", "2M+"), ("MTN MoMo Uganda", "10M+")],
    "RW": [("Bank of Kigali", "1M+"), ("Equity Rwanda", "500K+"), ("MTN MoMo Rwanda", "5M+")],
    "SN": [("CBAO", "1M+"), ("Société Générale Senegal", "500K+"), ("Orange Money Senegal", "5M+"), ("Wave Senegal", "8M+")],
    "CI": [("Société Générale CI", "1M+"), ("BICI", "500K+"), ("Orange Money CI", "8M+"), ("Wave CI", "10M+")],
    "CR": [("BAC Credomatic", "3M+"), ("Banco Nacional CR", "2M+"), ("Banco de Costa Rica", "1M+")],
    "PA": [("Banco General", "2M+"), ("BAC Panama", "1M+"), ("Banistmo", "1M+")],
    "DO": [("Banco Popular Dominicano", "5M+"), ("Banreservas", "5M+"), ("BHD León", "2M+")],
    "GT": [("Banco Industrial", "5M+"), ("Banrural", "5M+"), ("BAM Guatemala", "2M+")],
    "EC": [("Banco Pichincha", "5M+"), ("Banco del Pacífico", "2M+"), ("Banco Guayaquil", "2M+")],
    "VE": [("Banco de Venezuela", "10M+"), ("Banesco", "8M+"), ("Mercantil", "5M+")],
    "UY": [("BROU", "3M+"), ("Santander Uruguay", "1M+"), ("Itaú Uruguay", "500K+")],
    "BO": [("Banco Mercantil", "2M+"), ("BNB Bolivia", "1M+"), ("Banco BISA", "1M+")],
    "PY": [("Banco Itaú Paraguay", "1M+"), ("Banco Continental", "1M+"), ("Sudameris", "500K+")],
    "LK": [("Bank of Ceylon", "5M+"), ("Commercial Bank SL", "3M+"), ("Sampath Bank", "2M+"), ("HNB", "1M+")],
    "NP": [("Nepal Rastra Bank", "1M+"), ("Nabil Bank", "2M+"), ("Global IME", "2M+"), ("eSewa", "10M+")],
    "KH": [("ABA Bank", "5M+"), ("ACLEDA", "5M+"), ("Wing", "8M+")],
    "MM": [("KBZ Bank", "10M+"), ("AYA Bank", "5M+"), ("CB Bank", "3M+"), ("KBZ Pay", "15M+")],
    "MN": [("Khan Bank", "3M+"), ("Golomt Bank", "1M+"), ("TDB", "1M+")],
    "HK": [("HSBC HK", "5M+"), ("Hang Seng Bank", "3M+"), ("Standard Chartered HK", "2M+"), ("Bank of China HK", "3M+"), ("ZA Bank", "500K+"), ("Mox Bank", "300K+")],
    "TW": [("Cathay United", "10M+"), ("CTBC Bank", "8M+"), ("E.Sun Bank", "5M+"), ("Taipei Fubon", "5M+"), ("LINE Bank TW", "2M+")],
}

# ── App data by category ─────────────────────────────────────────────────
APPS: dict[str, list[dict]] = {
    "streaming": [
        {"name": "Spotify", "format": "JSON", "users": "574M+", "desc": "Music streaming history from GDPR data export"},
        {"name": "Netflix", "format": "CSV", "users": "283M+", "desc": "Viewing history from account data request"},
        {"name": "YouTube", "format": "JSON", "users": "2.7B+", "desc": "Watch history from Google Takeout"},
        {"name": "Apple Music", "format": "CSV/JSON", "users": "100M+", "desc": "Listening history from Privacy data request"},
        {"name": "Amazon Music", "format": "CSV", "users": "100M+", "desc": "Play history from Amazon data request"},
        {"name": "Tidal", "format": "CSV", "users": "10M+", "desc": "Streaming data from GDPR export"},
        {"name": "Deezer", "format": "CSV", "users": "16M+", "desc": "Listening history from account settings export"},
        {"name": "SoundCloud", "format": "JSON", "users": "76M+", "desc": "Listening and upload data from GDPR export"},
        {"name": "Pandora", "format": "CSV", "users": "50M+", "desc": "Listening history from data download"},
        {"name": "Disney+", "format": "CSV", "users": "150M+", "desc": "Viewing history from data download"},
        {"name": "Hulu", "format": "CSV", "users": "50M+", "desc": "Watch history from account data"},
        {"name": "HBO Max", "format": "CSV", "users": "76M+", "desc": "Viewing history from privacy data request"},
        {"name": "Amazon Prime Video", "format": "CSV", "users": "200M+", "desc": "Watch history from Amazon data request"},
        {"name": "Crunchyroll", "format": "CSV", "users": "13M+", "desc": "Watch history from data export"},
        {"name": "Plex", "format": "JSON/CSV", "users": "30M+", "desc": "Watch history from Tautulli or account data"},
        {"name": "Audible", "format": "CSV", "users": "200M+", "desc": "Listening history from Amazon data request"},
        {"name": "Kindle", "format": "CSV", "users": "100M+", "desc": "Reading history and highlights from data export"},
        {"name": "Goodreads", "format": "CSV", "users": "150M+", "desc": "Reading list and reviews from export"},
        {"name": "Pocket", "format": "CSV", "users": "30M+", "desc": "Saved articles from data export"},
        {"name": "Letterboxd", "format": "CSV", "users": "13M+", "desc": "Film diary and ratings from export"},
        {"name": "Last.fm", "format": "CSV", "users": "5M+", "desc": "Scrobble history from data export"},
        {"name": "Trakt", "format": "CSV/JSON", "users": "5M+", "desc": "Watch history from data export"},
    ],
    "health": [
        {"name": "Garmin Connect", "format": "FIT/CSV", "users": "30M+", "desc": "Activity and health data from Garmin export"},
        {"name": "Fitbit", "format": "JSON/CSV", "users": "31M+", "desc": "Activity, sleep, and health data from Google Takeout"},
        {"name": "Strava", "format": "GPX/FIT/CSV", "users": "125M+", "desc": "Activity data from bulk export or GDPR request"},
        {"name": "WHOOP", "format": "CSV", "users": "2M+", "desc": "Recovery, strain, and sleep data from web export"},
        {"name": "Oura Ring", "format": "CSV/JSON", "users": "1M+", "desc": "Sleep, activity, readiness from Oura Cloud export"},
        {"name": "Samsung Health", "format": "CSV", "users": "100M+", "desc": "Steps, sleep, HR data from app export"},
        {"name": "Google Fit", "format": "JSON", "users": "50M+", "desc": "Activity data from Google Takeout"},
        {"name": "Withings", "format": "CSV", "users": "10M+", "desc": "Weight, BP, sleep, activity from Health Mate export"},
        {"name": "Polar", "format": "CSV/TCX", "users": "5M+", "desc": "Training and sleep data from Polar Flow export"},
        {"name": "Suunto", "format": "FIT/GPX", "users": "3M+", "desc": "Activity data from Suunto app export"},
        {"name": "Coros", "format": "FIT", "users": "2M+", "desc": "Activity data from Coros app export"},
        {"name": "Amazfit/Zepp", "format": "CSV", "users": "10M+", "desc": "Activity and sleep data from Zepp app export"},
        {"name": "Peloton", "format": "CSV", "users": "7M+", "desc": "Workout history from account CSV download"},
        {"name": "Zwift", "format": "FIT", "users": "5M+", "desc": "Ride data from activity history"},
        {"name": "MyFitnessPal", "format": "CSV", "users": "200M+", "desc": "Nutrition and exercise logs from premium export"},
        {"name": "Cronometer", "format": "CSV", "users": "5M+", "desc": "Nutrition tracking data from export"},
        {"name": "LoseIt", "format": "CSV", "users": "40M+", "desc": "Food and weight log from data export"},
        {"name": "Headspace", "format": "CSV", "users": "70M+", "desc": "Meditation session history from data request"},
        {"name": "Calm", "format": "JSON", "users": "100M+", "desc": "Meditation and sleep data from GDPR request"},
        {"name": "Sleep Cycle", "format": "CSV", "users": "2M+", "desc": "Sleep analysis data from in-app export"},
        {"name": "Flo", "format": "CSV", "users": "300M+", "desc": "Period and ovulation tracking from data export"},
        {"name": "Clue", "format": "CSV", "users": "12M+", "desc": "Cycle tracking data from account export"},
        {"name": "Daylio", "format": "CSV", "users": "10M+", "desc": "Mood and activity journal from export"},
        {"name": "Dexcom CGM", "format": "CSV", "users": "2M+", "desc": "Continuous glucose monitor data from Clarity export"},
        {"name": "FreeStyle Libre", "format": "CSV", "users": "5M+", "desc": "Glucose readings from LibreView export"},
        {"name": "23andMe", "format": "TXT", "users": "12M+", "desc": "Raw DNA genotype data from account download"},
    ],
    "social": [
        {"name": "TikTok", "format": "JSON", "users": "1.5B+", "desc": "Activity data from Download Your Data (GDPR)"},
        {"name": "Snapchat", "format": "JSON", "users": "800M+", "desc": "Memories, chat, snaps from My Data export"},
        {"name": "Reddit", "format": "CSV", "users": "1.7B+", "desc": "Posts, comments, messages from GDPR request"},
        {"name": "Pinterest", "format": "JSON", "users": "480M+", "desc": "Pins, boards, activity from data download"},
        {"name": "Tumblr", "format": "JSON/HTML", "users": "135M+", "desc": "Posts and blog data from data export"},
        {"name": "Mastodon", "format": "CSV/JSON", "users": "10M+", "desc": "Posts and follows from account export"},
        {"name": "Bluesky", "format": "JSON", "users": "30M+", "desc": "Posts and data from account export (AT Protocol)"},
        {"name": "Threads", "format": "JSON", "users": "200M+", "desc": "Posts and activity from Instagram data download"},
        {"name": "BeReal", "format": "JSON", "users": "40M+", "desc": "Photo memories from data request"},
        {"name": "Strava Social", "format": "CSV", "users": "125M+", "desc": "Activity feed and kudos from export"},
        {"name": "Untappd", "format": "CSV", "users": "9M+", "desc": "Beer check-in history from data export"},
        {"name": "Vivino", "format": "CSV", "users": "65M+", "desc": "Wine ratings and reviews from data export"},
        {"name": "Foursquare/Swarm", "format": "JSON", "users": "50M+", "desc": "Check-in history from data export"},
        {"name": "VK", "format": "JSON", "users": "100M+", "desc": "Messages, posts, friends from data export"},
        {"name": "Weibo", "format": "JSON", "users": "580M+", "desc": "Posts and data from account export"},
    ],
    "payments": [
        {"name": "Klarna", "format": "CSV", "users": "150M+", "desc": "Purchase history and payments from data export"},
        {"name": "Afterpay", "format": "CSV", "users": "20M+", "desc": "BNPL payment history from account export"},
        {"name": "Affirm", "format": "CSV", "users": "18M+", "desc": "Loan and payment history from data download"},
        {"name": "Adyen", "format": "CSV", "users": "enterprise", "desc": "Payment reports from merchant dashboard export"},
        {"name": "Mollie", "format": "CSV", "users": "200K+", "desc": "Payment data from dashboard export"},
        {"name": "GoCardless", "format": "CSV", "users": "85K+", "desc": "Direct debit payment data from dashboard export"},
        {"name": "Payoneer", "format": "CSV", "users": "5M+", "desc": "Transaction history from account export"},
        {"name": "Skrill", "format": "CSV", "users": "40M+", "desc": "Transaction history from account export"},
        {"name": "Razorpay", "format": "CSV", "users": "10M+ merchants", "desc": "Payment data from dashboard export"},
        {"name": "PhonePe", "format": "CSV", "users": "500M+", "desc": "UPI transaction history from app statement"},
        {"name": "Paytm", "format": "CSV", "users": "350M+", "desc": "Payment and wallet history from data export"},
        {"name": "Google Pay", "format": "CSV", "users": "150M+", "desc": "Payment history from Google Takeout"},
        {"name": "Apple Pay", "format": "CSV", "users": "500M+", "desc": "Transaction data from Apple Wallet statements"},
        {"name": "Alipay", "format": "CSV", "users": "1.3B+", "desc": "Payment history from account bill download"},
        {"name": "WeChat Pay", "format": "CSV", "users": "900M+", "desc": "Payment records from bill export"},
        {"name": "GrabPay", "format": "CSV", "users": "50M+", "desc": "Payment history from app statement"},
        {"name": "GoPay", "format": "CSV", "users": "30M+", "desc": "Payment history from Gojek app export"},
        {"name": "Dana", "format": "CSV", "users": "50M+", "desc": "Payment history from app statement"},
        {"name": "ShopeePay", "format": "CSV", "users": "50M+", "desc": "Transaction history from Shopee export"},
        {"name": "GCash", "format": "CSV", "users": "80M+", "desc": "Transaction history from app statement"},
        {"name": "M-Pesa", "format": "CSV/PDF", "users": "50M+", "desc": "Mobile money transactions from statement download"},
        {"name": "Airtel Money", "format": "CSV", "users": "20M+", "desc": "Mobile money transactions from statement"},
        {"name": "MTN Mobile Money", "format": "CSV", "users": "60M+", "desc": "Mobile money transactions from statement"},
        {"name": "Orange Money", "format": "CSV", "users": "20M+", "desc": "Mobile money transactions from statement"},
        {"name": "Wave", "format": "CSV", "users": "10M+", "desc": "Mobile money transactions from app export"},
        {"name": "Flutterwave", "format": "CSV", "users": "enterprise", "desc": "Payment data from merchant dashboard export"},
        {"name": "Paystack", "format": "CSV", "users": "enterprise", "desc": "Payment data from merchant dashboard export"},
        {"name": "Mercado Pago", "format": "CSV", "users": "50M+", "desc": "Payment history from account export"},
        {"name": "PIX", "format": "CSV", "users": "150M+", "desc": "Instant payment records from bank app export"},
        {"name": "SumUp", "format": "CSV", "users": "4M+", "desc": "POS transaction reports from dashboard"},
    ],
    "crypto": [
        {"name": "Coinbase", "format": "CSV", "users": "110M+", "desc": "Transaction and tax history from account export"},
        {"name": "Binance", "format": "CSV", "users": "150M+", "desc": "Trade, deposit, withdrawal history from export"},
        {"name": "Kraken", "format": "CSV", "users": "10M+", "desc": "Trades and ledger from history export"},
        {"name": "Gemini", "format": "CSV/XLSX", "users": "13M+", "desc": "Transaction history from account statements"},
        {"name": "Bitstamp", "format": "CSV", "users": "4M+", "desc": "Trade history from account export"},
        {"name": "KuCoin", "format": "CSV", "users": "30M+", "desc": "Trade and transaction history from export"},
        {"name": "OKX", "format": "CSV", "users": "50M+", "desc": "Trade history from account export"},
        {"name": "Bybit", "format": "CSV", "users": "20M+", "desc": "Trade and transaction history from export"},
        {"name": "Crypto.com", "format": "CSV", "users": "80M+", "desc": "Transaction history from app export"},
        {"name": "Uniswap", "format": "CSV", "users": "17M+", "desc": "DEX swap history from Etherscan export"},
        {"name": "MetaMask", "format": "CSV", "users": "30M+", "desc": "Transaction history from Etherscan/chain explorer"},
        {"name": "Bitpanda", "format": "CSV", "users": "4M+", "desc": "Trade history from account export"},
        {"name": "Bitvavo", "format": "CSV", "users": "1M+", "desc": "Trade history from account export"},
        {"name": "eToro", "format": "XLSX", "users": "30M+", "desc": "Trading history from account statement"},
        {"name": "BlockFi", "format": "CSV", "users": "1M+", "desc": "Transaction history from account export"},
        {"name": "Luno", "format": "CSV", "users": "13M+", "desc": "Transaction history from account export"},
        {"name": "SwissBorg", "format": "CSV", "users": "800K+", "desc": "Portfolio and transaction data from export"},
        {"name": "Koinly", "format": "CSV", "users": "1M+", "desc": "Tax report and transaction data from export"},
        {"name": "CoinTracker", "format": "CSV", "users": "1M+", "desc": "Portfolio and tax data from export"},
    ],
    "ecommerce": [
        {"name": "eBay", "format": "CSV", "users": "135M+", "desc": "Purchase and selling history from account export"},
        {"name": "Etsy", "format": "CSV", "users": "96M+", "desc": "Order history and shop data from account export"},
        {"name": "AliExpress", "format": "CSV", "users": "150M+", "desc": "Order history from account data export"},
        {"name": "Walmart", "format": "CSV", "users": "120M+", "desc": "Order history from account export"},
        {"name": "Zalando", "format": "CSV", "users": "51M+", "desc": "Order history from GDPR data export"},
        {"name": "Bol.com", "format": "CSV", "users": "13M+", "desc": "Order history from account data export"},
        {"name": "Flipkart", "format": "CSV", "users": "450M+", "desc": "Order history from account data download"},
        {"name": "Coupang", "format": "CSV", "users": "30M+", "desc": "Order history from account export"},
        {"name": "Rakuten", "format": "CSV", "users": "80M+", "desc": "Order history from account export"},
        {"name": "Mercado Libre", "format": "CSV", "users": "148M+", "desc": "Order history from account export"},
        {"name": "Shopee", "format": "CSV", "users": "350M+", "desc": "Order history from app export"},
        {"name": "Lazada", "format": "CSV", "users": "160M+", "desc": "Order history from account export"},
        {"name": "Jumia", "format": "CSV", "users": "10M+", "desc": "Order history from account export"},
        {"name": "Takealot", "format": "CSV", "users": "5M+", "desc": "Order history from account export"},
        {"name": "WooCommerce", "format": "CSV", "users": "5M+ stores", "desc": "Order and product data from WordPress export"},
        {"name": "Gumroad", "format": "CSV", "users": "100K+", "desc": "Sales data from creator dashboard export"},
        {"name": "Lemon Squeezy", "format": "CSV", "users": "50K+", "desc": "Sales and subscription data from dashboard"},
    ],
    "travel": [
        {"name": "Uber", "format": "CSV", "users": "150M+", "desc": "Trip history and receipts from privacy data download"},
        {"name": "Lyft", "format": "CSV", "users": "20M+", "desc": "Ride history from data download request"},
        {"name": "Bolt", "format": "CSV", "users": "150M+", "desc": "Trip history from GDPR data request"},
        {"name": "Grab", "format": "CSV", "users": "250M+", "desc": "Trip and food order history from data export"},
        {"name": "DiDi", "format": "CSV", "users": "550M+", "desc": "Trip history from app data export"},
        {"name": "Google Maps Timeline", "format": "JSON", "users": "1B+", "desc": "Location history from Google Takeout"},
        {"name": "Booking.com", "format": "CSV", "users": "300M+", "desc": "Booking history from GDPR data request"},
        {"name": "Airbnb", "format": "CSV", "users": "150M+", "desc": "Booking and hosting data from account data request"},
        {"name": "Flixbus", "format": "CSV", "users": "60M+", "desc": "Trip history from account export"},
        {"name": "Trainline", "format": "CSV", "users": "25M+", "desc": "Booking history from account data"},
        {"name": "Lime", "format": "CSV", "users": "50M+", "desc": "Ride history from GDPR data request"},
    ],
    "food": [
        {"name": "Uber Eats", "format": "CSV", "users": "100M+", "desc": "Order history from Uber data download"},
        {"name": "DoorDash", "format": "CSV", "users": "37M+", "desc": "Order history from data request"},
        {"name": "Deliveroo", "format": "CSV", "users": "8M+", "desc": "Order history from GDPR data request"},
        {"name": "Just Eat", "format": "CSV", "users": "100M+", "desc": "Order history from GDPR export"},
        {"name": "Glovo", "format": "CSV", "users": "25M+", "desc": "Order history from GDPR data request"},
        {"name": "Rappi", "format": "CSV", "users": "45M+", "desc": "Order history from data request"},
        {"name": "iFood", "format": "CSV", "users": "50M+", "desc": "Order history from app data export"},
        {"name": "Swiggy", "format": "CSV", "users": "50M+", "desc": "Order history from data export"},
        {"name": "Zomato", "format": "CSV", "users": "80M+", "desc": "Order and review history from data export"},
        {"name": "Wolt", "format": "CSV", "users": "30M+", "desc": "Order history from GDPR data request"},
        {"name": "Thuisbezorgd", "format": "CSV", "users": "5M+", "desc": "Order history from GDPR data request"},
    ],
    "productivity": [
        {"name": "Notion", "format": "MD/CSV", "users": "35M+", "desc": "Pages and databases from workspace export"},
        {"name": "Evernote", "format": "ENEX/XML", "users": "225M+", "desc": "Notes and notebooks from ENEX export"},
        {"name": "Obsidian", "format": "MD", "users": "5M+", "desc": "Vault of Markdown files with YAML frontmatter"},
        {"name": "Todoist", "format": "CSV", "users": "35M+", "desc": "Task and project data from backup export"},
        {"name": "Trello", "format": "JSON", "users": "50M+", "desc": "Board and card data from JSON export"},
        {"name": "Asana", "format": "CSV/JSON", "users": "140M+", "desc": "Task and project data from export"},
        {"name": "ClickUp", "format": "CSV", "users": "10M+", "desc": "Task data from space/list export"},
        {"name": "Monday.com", "format": "XLSX", "users": "225K+", "desc": "Board data from Excel export"},
        {"name": "Airtable", "format": "CSV", "users": "300K+", "desc": "Base data from CSV export"},
        {"name": "Toggl Track", "format": "CSV", "users": "5M+", "desc": "Time tracking data from report export"},
        {"name": "Clockify", "format": "CSV", "users": "5M+", "desc": "Time entries from report export"},
        {"name": "RescueTime", "format": "CSV", "users": "2M+", "desc": "Screen time and productivity data from export"},
    ],
    "email": [
        {"name": "Gmail", "format": "MBOX", "users": "1.8B+", "desc": "Email archive from Google Takeout"},
        {"name": "Outlook/Hotmail", "format": "PST/CSV", "users": "400M+", "desc": "Email archive from Outlook export"},
        {"name": "Apple Mail", "format": "MBOX", "users": "100M+", "desc": "Email archive from Mail.app export"},
        {"name": "ProtonMail", "format": "MBOX/EML", "users": "100M+", "desc": "Email archive from ProtonMail export"},
        {"name": "Thunderbird", "format": "MBOX", "users": "20M+", "desc": "Email archive from profile data"},
        {"name": "Yahoo Mail", "format": "MBOX", "users": "225M+", "desc": "Email archive from data request"},
        {"name": "Fastmail", "format": "MBOX/EML", "users": "1M+", "desc": "Email archive from account export"},
        {"name": "Zoho Mail", "format": "EML", "users": "15M+", "desc": "Email data from mailbox export"},
    ],
    "accounting": [
        {"name": "QuickBooks", "format": "CSV/QBO", "users": "7M+", "desc": "Accounting data from chart of accounts and report exports"},
        {"name": "Xero", "format": "CSV", "users": "4M+", "desc": "Accounting data from report and bank statement export"},
        {"name": "FreshBooks", "format": "CSV", "users": "30M+", "desc": "Invoice and expense data from report export"},
        {"name": "Wave Accounting", "format": "CSV", "users": "4M+", "desc": "Transaction and invoice data from export"},
        {"name": "Sage", "format": "CSV", "users": "6M+", "desc": "Accounting data from report export"},
        {"name": "Moneybird", "format": "CSV", "users": "200K+", "desc": "Dutch accounting data from export"},
        {"name": "e-Boekhouden", "format": "CSV", "users": "100K+", "desc": "Dutch accounting data from export"},
        {"name": "Exact Online", "format": "CSV/XML", "users": "600K+", "desc": "Dutch/BE accounting and ERP data from export"},
        {"name": "MYOB", "format": "CSV", "users": "1.2M+", "desc": "Australian accounting data from export"},
        {"name": "Tally", "format": "XML/CSV", "users": "7M+", "desc": "Indian accounting data from export"},
        {"name": "Odoo", "format": "CSV/XLSX", "users": "12M+", "desc": "ERP and accounting data from export"},
    ],
    "telecom": [
        {"name": "AT&T", "format": "CSV/PDF", "users": "70M+", "country": "US", "desc": "Call, text, data usage from account export"},
        {"name": "Verizon", "format": "CSV/PDF", "users": "120M+", "country": "US", "desc": "Usage data from My Verizon export"},
        {"name": "T-Mobile US", "format": "CSV/PDF", "users": "120M+", "country": "US", "desc": "Usage data from account export"},
        {"name": "Vodafone", "format": "CSV", "users": "300M+", "country": "INTL", "desc": "Call and data usage from account export"},
        {"name": "Orange", "format": "CSV", "users": "280M+", "country": "INTL", "desc": "Call and data records from account export"},
        {"name": "Deutsche Telekom", "format": "CSV", "users": "250M+", "country": "DE", "desc": "Usage data from Kundencenter export"},
        {"name": "KPN", "format": "CSV", "users": "5M+", "country": "NL", "desc": "Call and data records from Mijn KPN export"},
        {"name": "Ziggo", "format": "CSV", "users": "4M+", "country": "NL", "desc": "Service usage data from account export"},
        {"name": "BT Group", "format": "CSV", "users": "20M+", "country": "GB", "desc": "Call records from BT account export"},
        {"name": "EE", "format": "CSV", "users": "15M+", "country": "GB", "desc": "Usage data from My EE export"},
        {"name": "Jio", "format": "CSV", "users": "460M+", "country": "IN", "desc": "Call and data usage from MyJio export"},
        {"name": "Airtel India", "format": "CSV", "users": "370M+", "country": "IN", "desc": "Usage data from Airtel Thanks export"},
    ],
    "gaming": [
        {"name": "Steam", "format": "JSON/CSV", "users": "130M+", "desc": "Game library, playtime, purchase history from account data"},
        {"name": "PlayStation", "format": "CSV", "users": "110M+", "desc": "Game and trophy history from data request"},
        {"name": "Xbox", "format": "CSV", "users": "100M+", "desc": "Game and achievement history from data request"},
        {"name": "Nintendo", "format": "CSV", "users": "120M+", "desc": "Play activity and purchase history from account data"},
        {"name": "Epic Games", "format": "JSON", "users": "270M+", "desc": "Game library and purchase data from data request"},
        {"name": "GOG", "format": "JSON", "users": "20M+", "desc": "Game library data from account export"},
        {"name": "Twitch", "format": "CSV", "users": "140M+", "desc": "Chat history and viewing data from GDPR request"},
        {"name": "Riot Games", "format": "JSON", "users": "180M+", "desc": "Game history and stats from data request"},
    ],
    "devtools": [
        {"name": "GitHub", "format": "JSON/CSV", "users": "100M+", "desc": "Repository, issue, PR data from API or archive"},
        {"name": "GitLab", "format": "JSON", "users": "30M+", "desc": "Project and issue data from export"},
        {"name": "Jira", "format": "CSV/XML", "users": "10M+", "desc": "Issue and project data from export"},
        {"name": "Linear", "format": "CSV/JSON", "users": "1M+", "desc": "Issue and project data from export"},
        {"name": "Sentry", "format": "JSON/CSV", "users": "4M+", "desc": "Error and performance data from export"},
        {"name": "Vercel", "format": "JSON", "users": "1M+", "desc": "Deployment and analytics data from API export"},
        {"name": "AWS Billing", "format": "CSV", "users": "1M+", "desc": "Cost and usage reports from S3 export"},
        {"name": "GCP Billing", "format": "CSV", "users": "500K+", "desc": "Cost reports from BigQuery export"},
        {"name": "Azure Billing", "format": "CSV", "users": "500K+", "desc": "Cost management data from export"},
    ],
    "chat": [
        {"name": "Slack", "format": "JSON", "users": "32M+", "desc": "Workspace messages and channels from export"},
        {"name": "Microsoft Teams", "format": "JSON", "users": "320M+", "desc": "Chat history from data export"},
        {"name": "WeChat", "format": "JSON/CSV", "users": "1.3B+", "desc": "Chat history from app backup"},
        {"name": "LINE", "format": "TXT", "users": "200M+", "desc": "Chat history from in-app export"},
        {"name": "KakaoTalk", "format": "TXT/CSV", "users": "53M+", "desc": "Chat history from in-app export"},
        {"name": "Viber", "format": "CSV", "users": "260M+", "desc": "Chat history from app backup export"},
        {"name": "Threema", "format": "JSON", "users": "12M+", "desc": "Chat data from app backup"},
        {"name": "Skype", "format": "JSON", "users": "300M+", "desc": "Chat history from data export"},
        {"name": "Google Chat", "format": "JSON", "users": "200M+", "desc": "Chat history from Google Takeout"},
        {"name": "Zoom Chat", "format": "TXT/CSV", "users": "350M+", "desc": "Chat logs from meeting recordings or export"},
        {"name": "Mattermost", "format": "JSON", "users": "2M+", "desc": "Channel data from bulk export"},
        {"name": "Zalo", "format": "TXT", "users": "75M+", "desc": "Chat history from app backup"},
    ],
    "realestate": [
        {"name": "Zillow", "format": "CSV", "users": "200M+", "country": "US", "desc": "Property listing and search data"},
        {"name": "Redfin", "format": "CSV", "users": "50M+", "country": "US", "desc": "Property listing and agent data"},
        {"name": "Funda", "format": "CSV", "users": "5M+", "country": "NL", "desc": "Dutch property listing data"},
        {"name": "Rightmove", "format": "CSV", "users": "20M+", "country": "GB", "desc": "UK property listing data"},
        {"name": "Idealista", "format": "CSV", "users": "15M+", "country": "ES", "desc": "Spanish property listing data"},
        {"name": "Immobilienscout24", "format": "CSV", "users": "15M+", "country": "DE", "desc": "German property listing data"},
        {"name": "SeLoger", "format": "CSV", "users": "10M+", "country": "FR", "desc": "French property listing data"},
        {"name": "Domain.com.au", "format": "CSV", "users": "8M+", "country": "AU", "desc": "Australian property listing data"},
        {"name": "Hemnet", "format": "CSV", "users": "3M+", "country": "SE", "desc": "Swedish property listing data"},
    ],
    "government": [
        {"name": "IRS Tax Return", "format": "PDF/CSV", "users": "150M+", "country": "US", "desc": "US tax return and transcript data"},
        {"name": "HMRC Self Assessment", "format": "CSV", "users": "12M+", "country": "GB", "desc": "UK tax return and payment data"},
        {"name": "Belastingdienst", "format": "CSV/XML", "users": "10M+", "country": "NL", "desc": "Dutch tax filing and assessment data"},
        {"name": "Finanzamt", "format": "CSV", "users": "30M+", "country": "DE", "desc": "German tax and ELSTER data"},
        {"name": "Receita Federal", "format": "CSV", "users": "40M+", "country": "BR", "desc": "Brazilian tax filing data"},
        {"name": "ATO", "format": "CSV", "users": "14M+", "country": "AU", "desc": "Australian tax return data"},
        {"name": "CRA", "format": "CSV", "users": "30M+", "country": "CA", "desc": "Canadian tax and benefit data"},
        {"name": "Agenzia delle Entrate", "format": "CSV/XML", "users": "40M+", "country": "IT", "desc": "Italian tax filing data"},
        {"name": "AEAT", "format": "CSV", "users": "20M+", "country": "ES", "desc": "Spanish tax filing data"},
        {"name": "Income Tax India", "format": "CSV/JSON", "users": "70M+", "country": "IN", "desc": "Indian ITR filing data"},
        {"name": "NTA Japan", "format": "CSV", "users": "30M+", "country": "JP", "desc": "Japanese tax filing data"},
    ],
    "insurance": [
        {"name": "AXA", "format": "PDF/CSV", "users": "95M+", "country": "INTL", "desc": "Policy and claims data from portal export"},
        {"name": "Allianz", "format": "PDF/CSV", "users": "126M+", "country": "INTL", "desc": "Insurance policy and claims data"},
        {"name": "State Farm", "format": "CSV", "users": "85M+", "country": "US", "desc": "Policy and claims data from account"},
        {"name": "Centraal Beheer", "format": "CSV", "users": "3M+", "country": "NL", "desc": "Dutch insurance policy data from Mijn CB export"},
        {"name": "Nationale-Nederlanden", "format": "CSV", "users": "4M+", "country": "NL", "desc": "Dutch insurance and pension data export"},
    ],
    "hr": [
        {"name": "ADP", "format": "CSV", "users": "1M+ companies", "desc": "Payroll and HR data from report export"},
        {"name": "Gusto", "format": "CSV", "users": "300K+", "desc": "Payroll data from report export"},
        {"name": "BambooHR", "format": "CSV", "users": "30K+", "desc": "Employee data from report export"},
        {"name": "Workday", "format": "CSV/XLSX", "users": "60M+", "desc": "HR and payroll data from report export"},
        {"name": "Personio", "format": "CSV", "users": "12K+", "desc": "HR data from export"},
        {"name": "Deel", "format": "CSV", "users": "35K+", "desc": "Payroll and contractor data from export"},
        {"name": "NMBRS", "format": "CSV", "users": "100K+", "country": "NL", "desc": "Dutch payroll data from export"},
        {"name": "AFAS", "format": "CSV/XML", "users": "50K+", "country": "NL", "desc": "Dutch HR/payroll data from export"},
    ],
    "education": [
        {"name": "Duolingo", "format": "JSON", "users": "80M+", "desc": "Learning progress and streak data from GDPR export"},
        {"name": "Anki", "format": "CSV/APKG", "users": "10M+", "desc": "Flashcard and review data from deck export"},
        {"name": "Coursera", "format": "CSV", "users": "130M+", "desc": "Course progress and certificate data from export"},
        {"name": "Canvas LMS", "format": "CSV", "users": "30M+", "desc": "Grade and assignment data from gradebook export"},
        {"name": "Google Classroom", "format": "CSV", "users": "150M+", "desc": "Assignment and grade data from data export"},
        {"name": "Khan Academy", "format": "CSV", "users": "150M+", "desc": "Learning progress data from profile export"},
    ],
    "dating": [
        {"name": "Tinder", "format": "JSON", "users": "75M+", "desc": "Match, message, and usage data from GDPR request"},
        {"name": "Hinge", "format": "JSON", "users": "20M+", "desc": "Match and preference data from GDPR request"},
        {"name": "Bumble", "format": "JSON", "users": "45M+", "desc": "Match and message data from GDPR request"},
        {"name": "OkCupid", "format": "JSON", "users": "10M+", "desc": "Profile, message, and match data from GDPR request"},
    ],
    "loyalty": [
        {"name": "Flying Blue", "format": "CSV", "users": "20M+", "desc": "Air France-KLM miles and activity from export"},
        {"name": "Miles & More", "format": "CSV", "users": "30M+", "desc": "Lufthansa Group miles from account export"},
        {"name": "Marriott Bonvoy", "format": "CSV", "users": "190M+", "desc": "Hotel points and stay history from export"},
        {"name": "Hilton Honors", "format": "CSV", "users": "180M+", "desc": "Points and stay history from account export"},
        {"name": "IHG Rewards", "format": "CSV", "users": "100M+", "desc": "Points and stay history from account export"},
        {"name": "Albert Heijn Bonus", "format": "CSV", "users": "6M+", "country": "NL", "desc": "Grocery purchase data from Bonus app export"},
        {"name": "Tesco Clubcard", "format": "CSV", "users": "20M+", "country": "GB", "desc": "Purchase data from Clubcard export"},
    ],
    "iot": [
        {"name": "Tesla Vehicle", "format": "CSV/JSON", "users": "5M+", "desc": "Driving, charging, and energy data from account export"},
        {"name": "Home Assistant", "format": "CSV/JSON", "users": "1M+", "desc": "Sensor and automation data from database export"},
        {"name": "Enphase Solar", "format": "CSV", "users": "3M+", "desc": "Solar production data from MyEnlighten export"},
        {"name": "SolarEdge", "format": "CSV", "users": "3M+", "desc": "Solar production data from monitoring portal export"},
        {"name": "Nest Thermostat", "format": "JSON", "users": "10M+", "desc": "Temperature and energy data from Google Takeout"},
        {"name": "Shelly", "format": "CSV", "users": "1M+", "desc": "Energy monitoring data from Shelly Cloud export"},
    ],
}

# ── Template functions ───────────────────────────────────────────────────

def slugify(text: str) -> str:
    s = text.lower().strip()
    s = re.sub(r"[''`]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def make_bank_entry(bank_name: str, customers: str, country_code: str) -> dict:
    c = COUNTRIES[country_code]
    cname, flag, dfmt, dec, thou, curr, csym, lang = c
    slug = slugify(f"{bank_name}-{cname}")

    # Build unique gotchas based on country locale
    gotchas = []
    if dec == ",":
        gotchas.append(f"Amounts use comma as decimal separator (e.g. 1.250,00 means 1250.00)")
    if dfmt not in ("YYYY-MM-DD",):
        gotchas.append(f"Date format is {dfmt} — differs from ISO 8601 standard")
    if lang != "English":
        gotchas.append(f"Column headers and values may be in {lang}")
    if "YYYY" == dfmt[:4]:
        gotchas.append("Year-first date format can be confused with numeric IDs")
    if thou == ".":
        gotchas.append(f"Thousand separator is dot (1.000 means one thousand, not 1.0)")
    elif thou == " ":
        gotchas.append(f"Thousand separator is space (1 000 means one thousand)")
    if not gotchas:
        gotchas.append("Date format may vary between export methods")
    # Ensure at least 2
    if len(gotchas) < 2:
        gotchas.append(f"Transaction descriptions contain bank-specific codes and abbreviations")
    gotchas = gotchas[:4]

    dec_desc = "comma" if dec == "," else "dot"
    ai_misreads = (
        f"AI doesn't understand {cname}'s {dfmt} date format, "
        f"treats {dec_desc} decimals as the wrong value, "
        f"and can't interpret {lang}-language column headers or transaction codes."
    )
    readright_fixes = (
        f"Normalizes dates to ISO 8601, converts {dec_desc}-decimal amounts to standard notation, "
        f"translates {lang} field names to English, and annotates transaction types with clear labels."
    )

    # Check if this has an existing parser
    parser_id = None
    has_parser = False
    for pid, (pname, pcountry, pcat) in EXISTING_PARSERS.items():
        if pcat == "banking" and pcountry == country_code and pname.lower() in bank_name.lower():
            parser_id = pid
            has_parser = True
            break

    return {
        "slug": slug,
        "name": bank_name,
        "country_code": country_code,
        "country_name": cname,
        "flag": flag,
        "category": "banking",
        "subcategory": "retail_banking",
        "format": "CSV",
        "description": f"{bank_name} is one of {cname}'s major banks with {customers} customers. "
                       f"Their online banking exports transaction data as CSV files with {dfmt} dates, "
                       f"{dec_desc}-decimal amounts in {curr}, and {lang}-language headers.",
        "users": f"{customers} customers",
        "gotchas": gotchas,
        "ai_misreads": ai_misreads,
        "readright_fixes": readright_fixes,
        "common_fields": [
            {"name": "date", "type": "date", "description": "Transaction date"},
            {"name": "amount", "type": "decimal", "description": f"Transaction amount ({curr})"},
            {"name": "description", "type": "string", "description": "Transaction description"},
            {"name": "balance", "type": "decimal", "description": f"Account balance ({curr})"},
            {"name": "counterparty", "type": "string", "description": "Other party name or reference"},
        ],
        "has_parser": has_parser,
        "parser_id": parser_id,
    }


def make_app_entry(app: dict, category: str) -> dict:
    name = app["name"]
    fmt = app["format"]
    users = app["users"]
    desc = app["desc"]
    country = app.get("country", "INTL")
    slug = slugify(f"{name}-{fmt.split('/')[0].lower()}-export")

    # Check existing parser
    parser_id = None
    has_parser = False
    for pid, (pname, pcountry, pcat) in EXISTING_PARSERS.items():
        if pname.lower().startswith(name.lower().split()[0].lower()) and pcat == category:
            parser_id = pid
            has_parser = True
            break

    # Category-specific gotchas
    gotchas_map = {
        "streaming": [
            "Timestamps may be in UTC or local time without timezone indicator",
            "Duration fields use milliseconds (ms_played) not minutes",
            "Track/artist names can contain special characters and Unicode",
        ],
        "health": [
            "Timestamps may mix UTC and local timezone without clear indication",
            "Measurement units vary (metric vs imperial) without explicit labels",
            "Data gaps from device not worn create misleading aggregates",
        ],
        "social": [
            "Timestamps use various formats (epoch, ISO 8601, or custom)",
            "Message content may contain HTML entities or Unicode escape sequences",
            "Deleted or edited content may still appear in exports with status flags",
        ],
        "payments": [
            "Amounts may be in minor units (cents) or major units depending on export type",
            "Refunds and chargebacks may appear as separate rows without clear linking",
            "Multi-currency transactions may show both original and converted amounts",
        ],
        "crypto": [
            "Amounts use up to 18 decimal places for some tokens",
            "Timestamps are typically UTC but may not be labeled",
            "Transaction types (trade, transfer, staking, airdrop) use platform-specific names",
        ],
        "ecommerce": [
            "Order totals may or may not include shipping and tax",
            "Currency symbols vary by marketplace region",
            "Cancelled and returned orders may appear as separate entries",
        ],
        "travel": [
            "Timestamps mix pickup/dropoff timezone with account timezone",
            "Distances may be in km or miles depending on region",
            "Surge pricing and tips may be separate line items",
        ],
        "food": [
            "Item names may include modifiers and customizations in one field",
            "Delivery fees, service fees, and tips are often separate columns",
            "Restaurant names may vary between orders for the same location",
        ],
        "productivity": [
            "Internal IDs reference other items but aren't human-readable",
            "Rich text formatting (markdown, HTML) mixed with plain text",
            "Date fields may use creation date, modification date, or both",
        ],
        "email": [
            "MBOX format concatenates thousands of emails in a single file",
            "Headers contain encoded text (MIME, Base64, quoted-printable)",
            "Thread structure requires parsing References and In-Reply-To headers",
        ],
        "accounting": [
            "Account codes follow country-specific chart of accounts standards",
            "Debit/credit conventions vary by software and locale",
            "Multi-currency transactions may show base and foreign amounts",
        ],
        "telecom": [
            "Call durations may be in seconds, minutes, or HH:MM:SS format",
            "Phone numbers may or may not include country codes",
            "Data usage units vary (MB, GB, KB) across different columns",
        ],
        "gaming": [
            "Playtime may be in minutes, hours, or seconds without clear labels",
            "Game titles may differ between regions for the same game",
            "Purchase history may show prices in different currencies per transaction",
        ],
        "devtools": [
            "Timestamps use ISO 8601 but may or may not include timezone offset",
            "User references may use IDs, usernames, or emails inconsistently",
            "Status fields use platform-specific enum values",
        ],
        "chat": [
            "Message timestamps may be in sender's timezone or UTC",
            "Media references point to files that may not be included in export",
            "Reactions and edits may appear as separate entries or nested data",
        ],
        "realestate": [
            "Prices may include or exclude taxes depending on local convention",
            "Area measurements vary (m², sq ft, sq m, hectares)",
            "Address formats differ significantly by country",
        ],
        "government": [
            "Tax amounts follow country-specific rounding rules",
            "Field names use official terminology that differs from common usage",
            "Fiscal years may not align with calendar years",
        ],
        "insurance": [
            "Policy dates, claim dates, and payment dates are easily confused",
            "Premium amounts may be monthly, quarterly, or annual without labels",
            "Coverage types use industry-specific codes",
        ],
        "hr": [
            "Salary amounts may be gross, net, or total cost to employer",
            "Date formats follow local conventions for employment records",
            "Tax withholding fields use country-specific terminology",
        ],
        "education": [
            "Grade scales differ by institution and country (A-F, 1-10, percentage)",
            "Timestamps may track last activity or completion, not both",
            "Course identifiers are institution-specific and not standardized",
        ],
        "dating": [
            "Timestamps may not include timezone information",
            "Match data may include deleted or unmatched profiles",
            "Location data precision varies (city vs exact coordinates)",
        ],
        "loyalty": [
            "Points values differ between programs and aren't directly comparable",
            "Tier status and earning rates change retroactively in exports",
            "Partner transactions may show different merchant names than expected",
        ],
        "iot": [
            "Sensor readings may use different units without explicit labels",
            "Timestamps may be in device local time or UTC inconsistently",
            "Missing readings from connectivity gaps create misleading averages",
        ],
    }

    gotchas = gotchas_map.get(category, [
        "Export format may change between software versions",
        "Field names may differ between languages/locales",
    ])

    ai_misreads = (
        f"AI can't interpret {name}'s specific export format correctly — "
        f"it misreads timestamps, confuses field semantics, and doesn't understand "
        f"the platform-specific conventions in {fmt} exports."
    )
    readright_fixes = (
        f"ReadRight's parser understands {name}'s {fmt} export format, normalizes timestamps to ISO 8601, "
        f"maps platform-specific fields to standard names, and annotates every field with type and meaning."
    )

    country_name = ""
    flag_emoji = ""
    if country != "INTL":
        if country in COUNTRIES:
            country_name = COUNTRIES[country][0]
            flag_emoji = COUNTRIES[country][1]
    else:
        country_name = "International"
        flag_emoji = "🌐"

    return {
        "slug": slug,
        "name": name,
        "country_code": country,
        "country_name": country_name,
        "flag": flag_emoji,
        "category": category,
        "subcategory": category,
        "format": fmt,
        "description": desc,
        "users": users,
        "gotchas": gotchas[:4],
        "ai_misreads": ai_misreads,
        "readright_fixes": readright_fixes,
        "common_fields": [
            {"name": "timestamp", "type": "datetime", "description": "Event timestamp"},
            {"name": "type", "type": "string", "description": "Event or record type"},
            {"name": "value", "type": "string", "description": "Primary value or content"},
        ],
        "has_parser": has_parser,
        "parser_id": parser_id,
    }


# ── Also generate format variants for banks ──────────────────────────────
BANK_FORMATS = ["CSV", "PDF", "OFX", "XLSX"]


def make_bank_format_variants(base: dict) -> list[dict]:
    """Create additional format variants for a bank (PDF, OFX, XLSX)."""
    variants = []
    for fmt in BANK_FORMATS[1:]:  # Skip CSV (already generated)
        v = base.copy()
        v["slug"] = f"{base['slug']}-{fmt.lower()}"
        v["format"] = fmt
        v["description"] = base["description"].replace("CSV files", f"{fmt} files")
        v["has_parser"] = False
        v["parser_id"] = None
        fmt_gotchas = list(base["gotchas"])
        if fmt == "PDF":
            fmt_gotchas.insert(0, "PDF statements require OCR or text extraction — table structures vary")
        elif fmt == "OFX":
            fmt_gotchas.insert(0, "OFX uses XML-like format with bank-specific extensions")
        elif fmt == "XLSX":
            fmt_gotchas.insert(0, "Excel exports may use formatted cells that hide the actual data values")
        v["gotchas"] = fmt_gotchas[:4]
        variants.append(v)
    return variants


# ── Main generation ──────────────────────────────────────────────────────

def generate() -> list[dict]:
    catalog = []
    seen_slugs = set()

    def add(entry: dict):
        slug = entry["slug"]
        # Deduplicate
        if slug in seen_slugs:
            slug = f"{slug}-2"
            entry["slug"] = slug
        if slug in seen_slugs:
            return  # Skip true dupes
        seen_slugs.add(slug)
        catalog.append(entry)

    # 1. Banks (~3000+ with format variants)
    for country_code, banks in BANKS.items():
        for bank_name, customers in banks:
            base = make_bank_entry(bank_name, customers, country_code)
            add(base)
            for variant in make_bank_format_variants(base):
                add(variant)

    # 2. Apps by category
    for category, apps in APPS.items():
        for app in apps:
            add(make_app_entry(app, category))

    # 3. Generate country-specific entries for global apps
    # (e.g., "Amazon Orders Japan", "PayPal Germany")
    global_apps_by_country = [
        ("Amazon", "ecommerce", "CSV"),
        ("PayPal", "payments", "CSV"),
        ("Uber", "travel", "CSV"),
        ("Uber Eats", "food", "CSV"),
        ("Airbnb", "travel", "CSV"),
        ("Netflix", "streaming", "CSV"),
        ("Spotify", "streaming", "JSON"),
        ("Google Pay", "payments", "CSV"),
        ("WhatsApp", "chat", "TXT"),
        ("Instagram", "social", "JSON"),
        ("TikTok", "social", "JSON"),
        ("Facebook", "social", "JSON"),
        ("Twitter/X", "social", "JSON"),
        ("LinkedIn", "social", "CSV"),
        ("Apple Health", "health", "XML"),
        ("Stripe", "payments", "CSV"),
        ("eBay", "ecommerce", "CSV"),
        ("YouTube", "streaming", "JSON"),
        ("Telegram", "chat", "JSON"),
        ("Discord", "chat", "CSV"),
        ("DoorDash", "food", "CSV"),
        ("Deliveroo", "food", "CSV"),
        ("Bolt", "travel", "CSV"),
        ("Grab", "travel", "CSV"),
        ("Booking.com", "travel", "CSV"),
        ("Fitbit", "health", "JSON"),
        ("Garmin", "health", "FIT"),
        ("Samsung Health", "health", "CSV"),
        ("Strava", "health", "GPX"),
        ("Steam", "gaming", "JSON"),
        ("Duolingo", "education", "JSON"),
        ("Wise", "payments", "CSV"),
        ("Revolut", "payments", "CSV"),
        ("N26", "payments", "CSV"),
        ("Coinbase", "crypto", "CSV"),
        ("Binance", "crypto", "CSV"),
        ("QuickBooks", "accounting", "CSV"),
        ("Xero", "accounting", "CSV"),
        ("Shopify", "ecommerce", "CSV"),
        ("WooCommerce", "ecommerce", "CSV"),
        ("Klarna", "payments", "CSV"),
        ("Snapchat", "social", "JSON"),
        ("Reddit", "social", "CSV"),
        ("Pinterest", "social", "JSON"),
        ("Notion", "productivity", "MD"),
        ("Slack", "chat", "JSON"),
        ("Microsoft Teams", "chat", "JSON"),
        ("Zoom", "chat", "CSV"),
        ("Tinder", "dating", "JSON"),
        ("Bumble", "dating", "JSON"),
        ("WHOOP", "health", "CSV"),
        ("Oura Ring", "health", "CSV"),
        ("Peloton", "health", "CSV"),
        ("MyFitnessPal", "health", "CSV"),
        ("Just Eat", "food", "CSV"),
        ("Glovo", "food", "CSV"),
        ("Swiggy", "food", "CSV"),
        ("Google Maps Timeline", "travel", "JSON"),
        ("Lime", "travel", "CSV"),
        ("PlayStation", "gaming", "CSV"),
        ("Xbox", "gaming", "CSV"),
        ("Epic Games", "gaming", "JSON"),
        ("Twitch", "gaming", "CSV"),
    ]
    ALL_COUNTRY_CODES = list(COUNTRIES.keys())
    for app_name, cat, fmt in global_apps_by_country:
        for cc in ALL_COUNTRY_CODES:
            if cc not in COUNTRIES:
                continue
            c = COUNTRIES[cc]
            cname, flag, dfmt, dec, thou, curr, csym, lang = c
            slug = slugify(f"{app_name}-{cname}")
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            dec_desc = "comma" if dec == "," else "dot"
            catalog.append({
                "slug": slug,
                "name": f"{app_name} {cname}",
                "country_code": cc,
                "country_name": cname,
                "flag": flag,
                "category": cat,
                "subcategory": f"{cat}_regional",
                "format": fmt,
                "description": f"{app_name} data exports for {cname} users include {lang}-language content, "
                               f"{curr} amounts with {dec_desc}-decimal notation, and {dfmt} date formats.",
                "users": "",
                "gotchas": [
                    f"Amounts in {curr} use {dec_desc} as decimal separator",
                    f"Dates follow {cname}'s {dfmt} format",
                    f"Content and labels may be in {lang}",
                ],
                "ai_misreads": f"AI doesn't know {cname}'s conventions — it misreads {dfmt} dates, "
                               f"confuses {dec_desc}-decimal {curr} amounts, and can't parse {lang} labels.",
                "readright_fixes": f"Normalizes {cname}-specific formatting to universal standards: "
                                   f"ISO 8601 dates, dot-decimal amounts, English field labels.",
                "common_fields": [
                    {"name": "date", "type": "date", "description": "Transaction/event date"},
                    {"name": "amount", "type": "decimal", "description": f"Amount in {curr}"},
                    {"name": "description", "type": "string", "description": "Item/transaction description"},
                ],
                "has_parser": False,
                "parser_id": None,
            })

    # 4. Generate additional country-specific banking use cases
    # "How to import [Country] bank statements into [Accounting Software]"
    accounting_tools = ["QuickBooks", "Xero", "Sage", "Moneybird", "MYOB", "Tally",
                        "FreshBooks", "Wave", "Odoo", "Exact Online", "ZohoBooks"]
    for cc in ALL_COUNTRY_CODES:
        if cc not in COUNTRIES:
            continue
        c = COUNTRIES[cc]
        cname, flag, dfmt, dec, thou, curr, csym, lang = c
        for tool in accounting_tools:
            slug = slugify(f"{cname}-bank-to-{tool}")
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            catalog.append({
                "slug": slug,
                "name": f"{cname} Bank Statement → {tool}",
                "country_code": cc,
                "country_name": cname,
                "flag": flag,
                "category": "banking",
                "subcategory": "bank_import",
                "format": "CSV",
                "description": f"Convert {cname} bank statements to {tool}-compatible format. "
                               f"Handles {dfmt} dates, {curr} amounts with {'comma' if dec == ',' else 'dot'} "
                               f"decimals, and {lang}-language headers.",
                "users": "",
                "gotchas": [
                    f"{cname} banks use {dfmt} dates — {tool} may expect a different format",
                    f"{'Comma' if dec == ',' else 'Dot'} decimal separator needs conversion for {tool}",
                    f"{lang} column headers must be mapped to {tool}'s expected field names",
                ],
                "ai_misreads": f"Importing {cname} bank data into {tool} fails because of date format "
                               f"mismatches ({dfmt} vs {tool}'s expected format), decimal separator conflicts, "
                               f"and {lang}-language headers that {tool} can't auto-map.",
                "readright_fixes": f"ReadRight normalizes {cname} bank exports to a universal format "
                                   f"that {tool} can import correctly — standardized dates, amounts, and field names.",
                "common_fields": [
                    {"name": "date", "type": "date", "description": "Transaction date"},
                    {"name": "amount", "type": "decimal", "description": f"Amount ({curr})"},
                    {"name": "payee", "type": "string", "description": "Counterparty name"},
                    {"name": "category", "type": "string", "description": "Transaction category"},
                ],
                "has_parser": False,
                "parser_id": None,
            })

    # 5. Format-specific landing pages
    formats = [
        ("CSV", "Comma-Separated Values — the most common data export format"),
        ("JSON", "JavaScript Object Notation — structured data with nesting"),
        ("XML", "Extensible Markup Language — used in banking (CAMT, OFX) and enterprise"),
        ("XLSX", "Microsoft Excel — formatted spreadsheets with multiple sheets"),
        ("PDF", "Portable Document Format — requires extraction/OCR for structured data"),
        ("OFX", "Open Financial Exchange — banking-specific XML format"),
        ("QIF", "Quicken Interchange Format — legacy personal finance format"),
        ("MBOX", "Unix mailbox format — concatenated email messages"),
        ("PST", "Outlook Personal Storage — Microsoft email archive format"),
        ("FIT", "Flexible and Interoperable Data Transfer — Garmin/ANT+ fitness data"),
        ("GPX", "GPS Exchange Format — route and waypoint data"),
        ("TCX", "Training Center XML — fitness activity data with HR/cadence"),
        ("ENEX", "Evernote Export Format — notes with attachments in XML"),
        ("VCF", "vCard Format — contact information"),
        ("ICS", "iCalendar — calendar events and appointments"),
        ("MT940", "SWIFT bank statement format — used in European banking"),
        ("CAMT.053", "ISO 20022 bank statement — modern banking standard"),
    ]
    for fmt_name, fmt_desc in formats:
        slug = slugify(f"{fmt_name}-file-parser")
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        catalog.append({
            "slug": slug,
            "name": f"{fmt_name} File Parser",
            "country_code": "INTL",
            "country_name": "International",
            "flag": "📄",
            "category": "format",
            "subcategory": "file_format",
            "format": fmt_name,
            "description": f"{fmt_desc}. ReadRight parses {fmt_name} files and wraps the data "
                           f"in a ContextEnvelope with schema annotations for AI.",
            "users": "",
            "gotchas": [
                f"{fmt_name} files from different sources use different conventions",
                "Character encoding varies (UTF-8, Latin-1, Windows-1252)",
                "Field delimiters and quoting rules may differ between exports",
            ],
            "ai_misreads": f"AI treats {fmt_name} as raw text, losing structure, types, and meaning. "
                           f"It can't distinguish dates from IDs, amounts from counts, or codes from names.",
            "readright_fixes": f"ReadRight auto-detects the source, parses the {fmt_name} structure, "
                               f"and outputs a typed, annotated ContextEnvelope that any AI reads correctly.",
            "common_fields": [
                {"name": "varies", "type": "varies", "description": f"Fields depend on the source of the {fmt_name} file"},
            ],
            "has_parser": False,
            "parser_id": None,
        })

    # 6. Use-case pages ("How to analyze X with AI")
    use_cases = [
        ("bank-statement-analysis", "Bank Statement Analysis with AI", "banking",
         "Upload your bank statements and let AI categorize transactions, find patterns, and create reports — without misreading your data."),
        ("expense-tracking-ai", "AI Expense Tracking from Bank Exports", "banking",
         "Automatically categorize expenses from any bank export. ReadRight ensures AI reads amounts, dates, and categories correctly."),
        ("chat-sentiment-analysis", "Chat Sentiment Analysis", "chat",
         "Analyze WhatsApp, Telegram, or Discord conversations with AI. ReadRight preserves timestamps, sender info, and message context."),
        ("health-data-trends", "Health & Fitness Data Trend Analysis", "health",
         "Track your health trends across Garmin, Fitbit, Apple Health, and more. ReadRight normalizes units and timestamps."),
        ("crypto-tax-reporting", "Crypto Tax Reporting", "crypto",
         "Generate tax reports from Coinbase, Binance, Kraken, and other exchanges. ReadRight handles multi-token, multi-currency transactions."),
        ("spotify-wrapped-diy", "DIY Spotify Wrapped", "streaming",
         "Create your own Spotify Wrapped with AI — analyze your full listening history without the limitations of Spotify's version."),
        ("personal-finance-dashboard", "Personal Finance Dashboard with AI", "banking",
         "Build a personal finance dashboard from your bank exports. ReadRight ensures AI reads every transaction correctly."),
        ("social-media-audit", "Social Media Data Audit", "social",
         "Audit your social media data from TikTok, Instagram, Twitter, and more. Understand your digital footprint."),
        ("email-analytics", "Email Analytics and Insights", "email",
         "Analyze years of email data from Gmail, Outlook, or ProtonMail. ReadRight parses MBOX/PST into structured data."),
        ("gdpr-data-export-analysis", "GDPR Data Export Analysis", "social",
         "Make sense of your GDPR data exports from any platform. ReadRight structures the raw dumps into AI-ready data."),
        ("ecommerce-order-analysis", "E-Commerce Order History Analysis", "ecommerce",
         "Analyze your purchase history across Amazon, eBay, Etsy, and more. Track spending patterns with AI."),
        ("payroll-data-analysis", "Payroll Data Analysis", "hr",
         "Analyze payroll exports from ADP, Gusto, or Workday. ReadRight normalizes salary, tax, and benefit data."),
        ("travel-expense-report", "Travel Expense Report from Uber/Booking", "travel",
         "Auto-generate travel expense reports from Uber, Bolt, Booking.com, and Airbnb exports."),
        ("dating-app-analysis", "Dating App Data Analysis", "dating",
         "Analyze your Tinder, Hinge, or Bumble data export. See match patterns, messaging stats, and usage trends."),
        ("gaming-playtime-analysis", "Gaming Playtime Analysis", "gaming",
         "Analyze your Steam, PlayStation, or Xbox gaming history. Track playtime, spending, and achievement progress."),
        ("food-delivery-spending", "Food Delivery Spending Analysis", "food",
         "Track your spending across Uber Eats, DoorDash, Deliveroo, and more. See where your money goes."),
        ("multi-bank-consolidation", "Multi-Bank Statement Consolidation", "banking",
         "Combine statements from multiple banks into one unified view. ReadRight handles different formats and currencies."),
        ("investment-portfolio-analysis", "Investment Portfolio Analysis", "crypto",
         "Consolidate your investment data across brokers and exchanges. Track performance across asset classes."),
        ("insurance-claims-tracking", "Insurance Claims Data Analysis", "insurance",
         "Analyze insurance policy and claims data. ReadRight handles multi-format exports from different insurers."),
        ("tax-preparation-data", "Tax Preparation Data Consolidation", "government",
         "Consolidate all your financial data for tax season. Bank statements, crypto trades, freelance income — all normalized."),
    ]
    for slug, name, cat, desc in use_cases:
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        catalog.append({
            "slug": slug,
            "name": name,
            "country_code": "INTL",
            "country_name": "International",
            "flag": "🎯",
            "category": "use_case",
            "subcategory": cat,
            "format": "Any",
            "description": desc,
            "users": "",
            "gotchas": [
                "Data comes from multiple sources with different formats and conventions",
                "Field names and types vary between platforms and countries",
                "AI needs explicit context to interpret each data source correctly",
            ],
            "ai_misreads": "Without context, AI guesses at field meanings, date formats, and number conventions — "
                           "leading to wrong calculations and invalid insights.",
            "readright_fixes": "ReadRight wraps each data source in a ContextEnvelope with typed schema, "
                               "conventions, and gotchas — so AI gets it right the first time.",
            "common_fields": [],
            "has_parser": False,
            "parser_id": None,
        })

    return catalog


def main():
    print("Generating source catalog...")
    catalog = generate()
    print(f"Generated {len(catalog)} entries")

    # Stats
    categories = {}
    countries = {}
    parsers = 0
    for entry in catalog:
        cat = entry["category"]
        categories[cat] = categories.get(cat, 0) + 1
        cc = entry["country_code"]
        countries[cc] = countries.get(cc, 0) + 1
        if entry["has_parser"]:
            parsers += 1

    print(f"\nCategories:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

    print(f"\nTop countries:")
    for cc, count in sorted(countries.items(), key=lambda x: -x[1])[:20]:
        name = COUNTRIES.get(cc, (cc,))[0] if cc in COUNTRIES else cc
        print(f"  {name}: {count}")

    print(f"\nWith existing parsers: {parsers}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(catalog, indent=1, ensure_ascii=False), encoding="utf-8")
    size_mb = OUTPUT.stat().st_size / (1024 * 1024)
    print(f"\nWritten to {OUTPUT} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
