from datetime import datetime
from enum import Enum

FORMAT_DATE = "%Y_%m_%d_%H_%M"
# DIR_ROOT = "/home/phuc/git/WikibaseSync"
DIR_ROOT = "/Users/phucnguyen/git/WikibaseSync"
DIR_LOG = f"{DIR_ROOT}/log/{datetime.now().strftime(FORMAT_DATE)}.txt"
DIR_INIT_ITEMS = f"{DIR_ROOT}/data/init_items.csv"
DIR_MAP_WD_COM_ID = f"{DIR_ROOT}/data/map_wd_com_id.csv"
DIR_DF_COM = f"{DIR_ROOT}/data/df_com"

NON_PAGE = "-1"

# Wikidata
WD_QUERY = {
    "url": "https://query.wikidata.org/bigdata/namespace/wdq/sparql",
    "prefix_pro": "http://www.wikidata.org/prop/direct/",
    "prefix_ent": "http://www.wikidata.org/entity/",
}

# Local Wikibase
WB_QUERY = {
    "url": "https://query.mtab.app/proxy/wdqs/bigdata/namespace/wdq/sparql",
    "prefix_pro": "https://wikicom.mtab.app/prop",
    "prefix_ent": "https://wikicom.mtab.app/entity",
}


class ItemAttribute(Enum):
    LABELS = "labels"
    DESCRIPTIONS = "descriptions"
    ALIASES = "aliases"
    SITELINKS = "sitelinks"
    CLAIMS = "claims"


class StatusSPARQL(Enum):
    Success = 0
    QueryBadFormed = 1
    Unauthorized = 2
    EndPointNotFound = 3
    URITooLong = 4
    EndPointInternalError = 5
    HTTPError = 6
    TooManyRequests = 7
    ConnectionError = 8


LANGS = ["en", "ja", "vi"]


# Init items

WC_INIT_ITEMS = [
    "P1630",  # formatter URL P3
    "P1921",  # formatter URI for RDF resource P4
    "P31",  # instance of P5
    "P159",  # headquarters location P6
    "P3225",  # Corporate Number (Japan) P7
    "Q4830453",  # business Q1
    "Q891723",  # public company Q2
    "Q786820",  # automobile manufacturer Q3
    "Q53268",  # Toyota Q4
]

# Total: 1066
SPARQL_JAPAN_COMPANIES_PROPS = {
    "query": 'SELECT DISTINCT ?p {?i wdt:P3225 ?id; ?p ?v. FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))}',
    "params": ["p"],
}

# Total: 2059
SPARQL_JAPAN_COMPANIES_TYPES = {
    "query": "SELECT DISTINCT ?t {?i wdt:P3225 ?id; wdt:P31/wdt:P279* ?t}",
    "params": ["t"],
}

# Total: 39996
SPARQL_JAPAN_COMPANIES_ITEMS = {
    "query": "SELECT DISTINCT ?i {?i wdt:P3225 ?id}",
    "params": ["i"],
}

SPARQL_COUNTRY_CITIES = {
    "query": "SELECT DISTINCT ?i {{?i wdt:P31/wdt:P279* wd:Q50337} UNION {?i wdt:P31/wdt:P279* wd:Q494721}}",
    "params": ["i"],
}

SPARQL_COUNTRY_JAPAN = {
    "query": "SELECT DISTINCT ?i {{?i wdt:P131* wd:Q17} UNION {?i wdt:P31/wdt:P279* wd:Q50337} UNION {?i wdt:P31/wdt:P279* wd:Q494721}}",
    "params": ["i"],
}

ATTRIBUTES_JA_COOP = [
    "sequenceNumber",
    "corporateNumber",  # P7
    "process",
    "correct",
    "updateDate",
    "changeDate",
    "name",  # Convert to
    "nameImageId",
    "kind",
    "prefectureName",
    "cityName",
    "streetNumber",
    "addressImageId",
    "prefectureCode",
    "cityCode",
    "postCode",
    "addressOutside",
    "addressOutsideImageId",
    "closeDate",
    "closeCause",
    "successorCorporateNumber",
    "changeCause",
    "assignmentDate",
    "latest",
    "enName",
    "enPrefectureName",
    "enCityName",
    "enAddressOutside",
    "furigana",
    "hihyoji",
]

# Wikibase language
# https://gerrit.wikimedia.org/r/changes/operations%2Fmediawiki-config~539162/revisions/3/files/langlist/download
# Some Wikidata language codes are not loaded in Wikibase
# https://phabricator.wikimedia.org/T236177
# LANGS = [
#     "aa",
#     "ab",
#     "ace",
#     "ady",
#     "af",
#     "ak",
#     "als",
#     "am",
#     "an",
#     "ang",
#     "ar",
#     "arc",
#     "arz",
#     "as",
#     "ast",
#     "atj",
#     "av",
#     "ay",
#     "az",
#     "azb",
#     "ba",
#     "bar",
#     "bat-smg",
#     "bcl",
#     "be",
#     "be-x-old",
#     "bg",
#     "bh",
#     "bi",
#     "bjn",
#     "bm",
#     "bn",
#     "bo",
#     "bpy",
#     "br",
#     "bs",
#     "bug",
#     "bxr",
#     "ca",
#     "cbk-zam",
#     "cdo",
#     "ce",
#     "ceb",
#     "ch",
#     "cho",
#     "chr",
#     "chy",
#     "ckb",
#     "co",
#     "cr",
#     "crh",
#     "cs",
#     "csb",
#     "cu",
#     "cv",
#     "cy",
#     "da",
#     "de",
#     "din",
#     "diq",
#     "dsb",
#     "dty",
#     "dv",
#     "dz",
#     "ee",
#     "el",
#     "eml",
#     "en",
#     "eo",
#     "es",
#     "et",
#     "eu",
#     "ext",
#     "fa",
#     "ff",
#     "fi",
#     "fiu-vro",
#     "fj",
#     "fo",
#     "fr",
#     "frp",
#     "frr",
#     "fur",
#     "fy",
#     "ga",
#     "gag",
#     "gan",
#     "gd",
#     "gl",
#     "glk",
#     "gn",
#     "gom",
#     "gor",
#     "got",
#     "gu",
#     "gv",
#     "ha",
#     "hak",
#     "haw",
#     "he",
#     "hi",
#     "hif",
#     "ho",
#     "hr",
#     "hsb",
#     "ht",
#     "hu",
#     "hy",
#     "hyw",
#     "hz",
#     "ia",
#     "id",
#     "ie",
#     "ig",
#     "ii",
#     "ik",
#     "ilo",
#     "inh",
#     "io",
#     "is",
#     "it",
#     "iu",
#     "ja",
#     "jam",
#     "jbo",
#     "jv",
#     "ka",
#     "kaa",
#     "kab",
#     "kbd",
#     "kbp",
#     "kg",
#     "ki",
#     "kj",
#     "kk",
#     "kl",
#     "km",
#     "kn",
#     "ko",
#     "koi",
#     "kr",
#     "krc",
#     "ks",
#     "ksh",
#     "ku",
#     "kv",
#     "kw",
#     "ky",
#     "la",
#     "lad",
#     "lb",
#     "lbe",
#     "lez",
#     "lfn",
#     "lg",
#     "li",
#     "lij",
#     "lmo",
#     "ln",
#     "lo",
#     "lrc",
#     "lt",
#     "ltg",
#     "lv",
#     "mai",
#     "map-bms",
#     "mdf",
#     "mg",
#     "mh",
#     "mhr",
#     "mi",
#     "min",
#     "mk",
#     "ml",
#     "mn",
#     "mo",
#     "mr",
#     "mrj",
#     "ms",
#     "mt",
#     "mus",
#     "mwl",
#     "my",
#     "myv",
#     "mzn",
#     "na",
#     "nah",
#     "nap",
#     "nds",
#     "nds-nl",
#     "ne",
#     "new",
#     "ng",
#     "nl",
#     "nn",
#     "no",
#     "nov",
#     "nqo",
#     "nrm",
#     "nso",
#     "nv",
#     "ny",
#     "oc",
#     "olo",
#     "om",
#     "or",
#     "os",
#     "pa",
#     "pag",
#     "pam",
#     "pap",
#     "pcd",
#     "pdc",
#     "pfl",
#     "pi",
#     "pih",
#     "pl",
#     "pms",
#     "pnb",
#     "pnt",
#     "ps",
#     "pt",
#     "qu",
#     "rm",
#     "rmy",
#     "rn",
#     "ro",
#     "roa-rup",
#     "roa-tara",
#     "ru",
#     "rue",
#     "rw",
#     "sa",
#     "sah",
#     "sat",
#     "sc",
#     "scn",
#     "sco",
#     "sd",
#     "se",
#     "sg",
#     "sh",
#     "shn",
#     "si",
#     "simple",
#     "sk",
#     "sl",
#     "sm",
#     "sn",
#     "so",
#     "sq",
#     "sr",
#     "srn",
#     "ss",
#     "st",
#     "stq",
#     "su",
#     "sv",
#     "sw",
#     "szl",
#     "ta",
#     "tcy",
#     "te",
#     "tet",
#     "tg",
#     "th",
#     "ti",
#     "tk",
#     "tl",
#     "tn",
#     "to",
#     "tpi",
#     "tr",
#     "ts",
#     "tt",
#     "tum",
#     "tw",
#     "ty",
#     "tyv",
#     "udm",
#     "ug",
#     "uk",
#     "ur",
#     "uz",
#     "ve",
#     "vec",
#     "vep",
#     "vi",
#     "vls",
#     "vo",
#     "wa",
#     "war",
#     "wo",
#     "wuu",
#     "xal",
#     "xh",
#     "xmf",
#     "yi",
#     "yo",
#     "yue",
#     "za",
#     "zea",
#     "zh",
#     "zh-classical",
#     "zh-min-nan",
#     "zh-yue",
#     "zu",
# ]
