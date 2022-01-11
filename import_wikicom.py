import others_config as cf
from collections import OrderedDict, defaultdict
from pywikibot.data import api
from utilities import io_worker as iw
from utilities.sparql_queries import SparqlEndPoint
from tqdm import tqdm
from utilities.util import WikibaseImporter
import pandas as pd
import pywikibot
import jaconv
import m_f
import pywikibot


def get_wikicom_init_items(refresh=False):
    # Use temp file
    if not refresh:
        try:
            init_list = iw.load_object_csv(cf.DIR_INIT_ITEMS)
            if init_list:
                init_list = [i[0] for i in init_list]
                return init_list
        except FileNotFoundError:
            pass
        except Exception as message:
            iw.print_status(message)
            pass

    # Refresh
    # Wikidata SPARQL endpoint
    sparql_wd = SparqlEndPoint(endpoint_config=cf.WD_QUERY)

    init_list = cf.WC_INIT_ITEMS

    def run_queries(type_obj, **kwargs):
        status, responds = sparql_wd.run(**kwargs)
        iw.print_status(f"{type_obj}: {len(responds)}")
        if status == cf.StatusSPARQL.Success:
            init_list.extend(sorted([l[0] for l in responds], key=lambda x: int(x[1:])))

    run_queries("Properties", **cf.SPARQL_JAPAN_COMPANIES_PROPS)
    run_queries("Types", **cf.SPARQL_JAPAN_COMPANIES_TYPES)
    run_queries("Item", **cf.SPARQL_JAPAN_COMPANIES_ITEMS)
    # old_list = [i[0] for i in iw.load_object_csv(cf.DIR_INIT_ITEMS)]
    run_queries("Cities", **cf.SPARQL_COUNTRY_CITIES)
    run_queries("Japan", **cf.SPARQL_COUNTRY_JAPAN)
    # init_list = list(set(init_list) - set(old_list))

    init_list = list(OrderedDict.fromkeys(init_list))
    iw.save_object_csv(cf.DIR_INIT_ITEMS, init_list)
    iw.print_status(f"Total: {len(init_list)}")
    return init_list


def import_init_list():
    m_f.init()
    # Wikibase
    wikibase_site = pywikibot.Site("my", "my")
    # Wikidata
    wikidata_site = pywikibot.Site("wikidata", "wikidata")

    importer = WikibaseImporter(wikibase_site, wikidata_site)

    def import_wikidata_items(list_items, from_i=0, to_i=None, import_claims=True):
        if to_i is None:
            to_i = len(list_items)
        for i, wikidata_id in enumerate(
            tqdm(list_items[from_i:to_i], total=len(list_items), initial=from_i)
        ):
            importer.change_item(
                importer.wikidata_repo,
                importer.wikibase_repo,
                wikidata_id,
                statements=import_claims,
            )

    # custom_list = [
    #     # "Q327333",
    #     # "Q1054813",
    #     # "Q1480166",
    #     # "Q11570722",
    #     # "Q5627756",
    #     # "Q17000076",
    #     # "Q1562410",
    #     "Q35581",
    # ]
    # import_wikidata_items(custom_list, import_claims=True)
    # return

    # import_wikidata_items(cf.WC_INIT_ITEMS, import_claims=False)
    # --> Add formatter URL for P1 P2
    # Import claims
    # import_wikidata_items(cf.WC_INIT_ITEMS, import_claims=True)
    full_list = get_wikicom_init_items(refresh=True)
    # Import first properties and types
    # import_wikidata_items(full_list[:3125], import_claims=False)
    # Import full list claims
    import_wikidata_items(full_list, from_i=0, import_claims=True)


def read_database_ja(refresh=False, release="20211130"):
    if not refresh:
        try:
            return pd.read_pickle(cf.DIR_DF_COM)
        except FileNotFoundError:
            pass
        except Exception as e:
            iw.print_status(e)
            pass
    # parse XML
    # /Users/phucnguyen/git/WikibaseSync/data/06_yamagata_all_20211130/06_yamagata_all_20211130.xml
    # tmp_df = pd.read_xml(
    #     f"/Users/phucnguyen/git/WikibaseSync/data/06_yamagata_all_20211130/06_yamagata_all_20211130.xml",
    #     parser="etree",
    # )
    # print(tmp_df.columns)
    # columns = tmp_df.columns
    dtypes_int = [0, 3, 23, 29]
    dtypes = {
        col: int if i in dtypes_int else str
        for i, col in enumerate(cf.ATTRIBUTES_JA_COOP)
    }
    parse_dates = ["updateDate", "changeDate", "closeDate", "assignmentDate"]

    tmp_df = pd.read_csv(
        f"{cf.DIR_ROOT}/data/00_zenkoku_all_{release}/00_zenkoku_all_{release}.csv",
        header=None,
        names=cf.ATTRIBUTES_JA_COOP,
        low_memory=False,
        dtype=dtypes,
        parse_dates=parse_dates,
    )
    tmp_df.to_pickle(cf.DIR_DF_COM)
    return tmp_df


def get_wd_ja_coop_id(refresh=False, get_detail_info=False):
    # Use temp file
    run_wd_query = True
    query_responds = []
    if not refresh:
        try:
            query_responds = iw.load_object_csv(cf.DIR_MAP_WD_COM_ID)
            run_wd_query = False
        except FileNotFoundError:
            pass
        except Exception as message:
            iw.print_status(message)
            pass
    if run_wd_query:
        # Wikidata company id
        kwargs_query = {
            "query": "SELECT DISTINCT ?i ?iLabel ?t ?tLabel ?id {"
            "?i wdt:P3225 ?id; wdt:P31 ?t."
            'SERVICE wikibase:label { bd:serviceParam wikibase:language "en".}'
            "}",
            "params": ["i", "iLabel", "t", "tLabel", "id"],
        }
        wd_query = SparqlEndPoint()
        status, query_responds = wd_query.run(**kwargs_query)
        if status != cf.StatusSPARQL.Success:
            raise Exception(status)
        print("Query: " + kwargs_query["query"])
        print(f"Responds: {len(query_responds)} records")
        iw.save_object_csv(cf.DIR_MAP_WD_COM_ID, query_responds)

    com_id = defaultdict(set)
    type_wd = defaultdict(set)
    count_type = 0
    items = {}
    for i, il, t, tl, id in query_responds:
        com_id[id].add(i)
        if get_detail_info:
            items[i] = il
            count_type += 1
            type_wd[f"{t}\t{tl}"].add(id)

    count_dup = 0
    mapper = {}
    print_dup = []
    for k, v in com_id.items():
        # Get the first edit item
        mapper[k] = sorted(list(v), key=lambda x: x[1:])[0]
        if get_detail_info and len(v) > 1:
            count_dup += len(v)
            print_dup.append([k, v])
    print(f"Mapper final: {len(mapper)}")

    if get_detail_info:
        iw.print_status(
            f"\tDuplicate: {count_dup}/{len(items)} - {count_dup / len(items) * 100:.2f}%"
        )
        for i, (k, v) in enumerate(print_dup):
            iw.print_status(
                f"{i + 1}\t"
                f"{k}\t"
                f"{len(v)}\t" + " ".join([f"{i}[{items.get(i)}]" for i in v])
            )

        type_wd = sorted(type_wd.items(), key=lambda x: len(x[1]), reverse=True)
        iw.print_status(
            f"\tTypes: {len(type_wd)} - Types/item: {count_type / len(items):.2f}"
        )
        for i, (k, v) in enumerate(type_wd):
            iw.print_status(f"{i + 1}\t{k}\t{len(v)}\t{len(v) / len(items) * 100:.2f}%")

    return mapper


def analysis_wikidata_and_ja_db():
    # Japanese coops
    coops_ja = read_database_ja(refresh=False, release="20211130")
    print(coops_ja.info())
    for c in cf.ATTRIBUTES_JA_COOP:
        print(f"\n{c}: ")
        print(coops_ja[c][coops_ja[c].notnull()])

    # Wikidata coops
    coops_wd = get_wd_ja_coop_id()

    # Analysis overlapping between wikidata and ja database
    coops_overlap = coops_ja.loc[
        coops_ja["corporateNumber"].isin({k for k in coops_wd})
    ]
    # coops_overlap.to_pickle(cf.DIR_DF_COM + ".mapped")
    print(coops_overlap.info())
    for c in cf.ATTRIBUTES_JA_COOP:
        print(f"\n{c}: ")
        print(coops_overlap[c][coops_overlap[c].notnull()])


def import_ja_coops(release="20211130"):
    # Japanese coops
    coops_ja = read_database_ja(refresh=False, release=release)

    temp = coops_ja.loc[coops_ja["corporateNumber"] == "5000020232114"]

    # Wikidata coops
    coops_wd = get_wd_ja_coop_id()

    coops_ja = coops_ja.loc[~coops_ja["corporateNumber"].isin({k for k in coops_wd})]

    for row in coops_ja.itertuples():
        # Corporate Number (Japan) (P7) <-- P3225
        c_corporateNumber = row["corporateNumber"]

        # name in native language (P350) <-- P1559
        # official name (P331) <-- P1448
        # label in Japanese
        c_name = jaconv.zenkaku2hankaku(row["name"], ascii=True)

        # label in english
        c_enName = row["enName"]

        # name in kana (P383) <-- P1814
        c_furigana = row["furigana"]

        # instance of (P5) <-- P31
        # all: Corporation (Japan) (Q1867) <-- (Q48748864)
        # 101: government agency (Q396) <--(Q327333)
        # 201: municipality of Japan (Q673) <-- (Q1054813)
        # 301: kabushiki gaisha (Q859) <-- (Q1480166)
        # 302: tokurēyūgen gaisha (Q16924) <-- (Q11570722)
        # 303: gōmei gaisha (Q64297) <-- (Q5627756)
        # 304: gōshi gaisha (Q53926) <-- (Q17000076)
        # 305: gōdō gaisha (Q50699) <-- (Q1562410)
        # 399: Corporation (Japan) (Q1867) <-- (Q48748864)
        # 401: Corporation (Japan) (Q1867) <-- (Q48748864)
        # 499: Corporation (Japan) (Q1867) <-- (Q48748864)
        if row["kind"] == "101":
            c_kind = ["Q396"]
        elif row["kind"] == "201":
            c_kind = ["Q673"]
        elif row["kind"] == "301":
            c_kind = ["Q859"]
        elif row["kind"] == "302":
            c_kind = ["Q16924"]
        elif row["kind"] == "303":
            c_kind = ["Q64297"]
        elif row["kind"] == "304":
            c_kind = ["Q53926"]
        elif row["kind"] == "305":
            c_kind = ["Q50699"]
        else:
            c_kind = []
        c_kind.append("Q1867")

        # inception (P164) <-- P571
        c_assignmentDate = row["assignmentDate"]

        # headquarters location (P6) - postal code (P103) <-- P159 - P281
        c_postCode = row["postCode"]

        c_prefectureName = row["prefectureName"]
        # headquarters location (P6) - located in the administrative territorial entity (P48) <-- P159 - P131
        c_cityName = row["cityName"]

        # headquarters location (P6) - street address (P814) <-- P159 - P6375
        c_streetNumber = row["streetNumber"]

        # date of official closure (P1644) <-- P576
        c_closeDate = row["closeDate"]

        # New property
        # P3057
        c_successorCorporateNumber = row["successorCorporateNumber"]

        # headquarters location (P6) - located in the administrative territorial entity (P48) <-- P159 - P131
        c_enPrefectureName = row["enPrefectureName"]

        # headquarters location (P6) - street address (P814) <-- P159 - P6375
        c_enCityName = row["enCityName"]
        # located in the administrative territorial entity (P48) - street address (P814) <-- P159 - P6375
        # P131 - P6375 Street address
        c_enAddressOutside = row["enAddressOutside"]

        # located in the administrative territorial entity (P48) - street address (P814) <-- P131 - P6375
        c_addressOutside = row["addressOutside"]


if __name__ == "__main__":
    # 1. Get wikicom init list: wikibase instance of Japanese companies
    # wc_init_items = get_wikicom_init_items(refresh=True)

    # 2. Import the init list
    import_init_list()

    # 3. Import hojin bango
    # import_ja_coops(release="20211130")

    # test_import()
