import pandas as pd
import pywikibot
from pandas import Timestamp
from tqdm import tqdm

import m_f
from import_wikicom import read_database_ja
from utilities.sparql_queries import SparqlEndPoint
import others_config as cf
from utilities.util import WikibaseImporter
from pywikibot.data import api
from utilities import io_worker as iw
import sys


def get_coop_by_number(coop_number):
    wc_sparql = SparqlEndPoint(cf.WB_QUERY)
    kwargs_query = {
        "query": 'SELECT DISTINCT ?item{?item wdt:P7 "%s"}' % coop_number,
        "params": ["item"],
    }
    status, responds = wc_sparql.run(**kwargs_query)
    if status == cf.StatusSPARQL.Success:
        responds = [wc_id[0].split("/")[-1] for wc_id in responds]
        if len(responds) == 1:
            return responds[0]
    return None


def get_items(site, item_title, lang="en"):
    """
    Requires a site and search term (item_title) and returns the results.
    """
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": lang,
        "type": "item",  # property
        "search": item_title,
    }
    request = api.Request(site=site, **params)
    return request.submit()


def check_claim(item, relation, value, lang="ja"):
    item_dict = item.get()
    if not item_dict.get("claims") or not item_dict.get("claims").get(relation):
        return None

    for claim in item_dict["claims"].get(relation):
        wb_data = claim.getTarget()
        if isinstance(wb_data, pywikibot.ItemPage):
            if wb_data.id == value:
                return claim
        elif isinstance(wb_data, pywikibot.WbMonolingualText):
            if wb_data.language == lang and wb_data.text == value:
                return claim
        else:
            if wb_data == value:
                return claim

    return None


def check_source(claim, source_data):
    source_claims = claim.getSources()
    if len(source_claims) == 0:
        return False

    for source in source_claims:
        if not source.get("P208"):
            continue
        for claim in source.get("P208"):
            if claim.target == source_data:
                return True
    return False


def set_claim(repo, item, property, value, message="", lang="ja"):
    wb_prop = pywikibot.PropertyPage(repo, property)
    item = pywikibot.ItemPage(repo, item.id)
    # item.get()
    claim = pywikibot.Claim(repo, property, wb_prop.type)

    # {
    #     "wikibase-item": ItemPage,
    #     # 'wikibase-property': PropertyPage, must be declared first
    #     "string": str,
    #     "commonsMedia": FilePage,
    #     "globe-coordinate": pywikibot.Coordinate,
    #     "url": str,
    #     "time": pywikibot.WbTime,
    #     "quantity": pywikibot.WbQuantity,
    #     "monolingualtext": pywikibot.WbMonolingualText,
    #     "math": str,
    #     "external-id": str,
    #     "geo-shape": pywikibot.WbGeoShape,
    #     "tabular-data": pywikibot.WbTabularData,
    #     "musical-notation": str,
    # }
    if wb_prop.type == "monolingualtext":
        claim.setTarget(pywikibot.WbMonolingualText(text=value, language=lang))
    elif wb_prop.type == "wikibase-item":
        if isinstance(value, str):
            value = pywikibot.ItemPage(repo, value)
        claim.setTarget(value)
    elif isinstance(value, Timestamp):
        claim.setTarget(
            pywikibot.WbTime(year=value.year, month=value.month, day=value.day)
        )
    else:
        # elif wb_prop.type in [
        #     "string",
        #     "url",
        #     "math",
        #     "external-id",
        #     "musical-notation",
        # ]:
        claim.setTarget(value)
    item.addClaim(claim, summary=f"Added {message}")
    return claim


def create_source_claim(repo, claim, source_data):
    source_claim = pywikibot.Claim(repo, "P208", isReference=True)
    source_claim.setTarget(source_data)
    try:
        claim.addSources([source_claim])
    except Exception as message:
        iw.print_status(message)
        return False
    return True


def test_import(release="20211130", from_i=0, to_i=-1):
    m_f.init()
    # Wikibase
    wikibase_site = pywikibot.Site("my", "my")
    # Wikidata
    wikidata_site = pywikibot.Site("wikidata", "wikidata")
    importer = WikibaseImporter(wikibase_site, wikidata_site)

    source_data_temp = (
        "https://www.houjin-bangou.nta.go.jp/henkorireki-johoto.html?selHouzinNo="
    )
    # Japanese coops
    coops_ja = read_database_ja(refresh=False, release=release)
    iw.print_status(f"Total: {len(coops_ja)}")
    # coops_ja = coops_ja.loc[coops_ja["corporateNumber"] == "1180301018771"]
    # coops_ja = coops_ja.loc[coops_ja["closeCause"].notnull()]
    if to_i == -1:
        to_i = len(coops_ja)

    coops_ja = coops_ja.iloc[from_i:to_i]

    def update_desc(message=""):
        return f"{from_i:,}:{to_i:,}. {message}"

    p_bar = tqdm(total=to_i - from_i, desc=update_desc())
    for item_i, item_example in enumerate(coops_ja.itertuples()):
        # if item_i < from_i:
        #     continue
        if not item_example.corporateNumber or pd.isnull(item_example.corporateNumber):
            continue

        source_data = source_data_temp + item_example.corporateNumber
        found_item = get_coop_by_number(item_example.corporateNumber)

        if found_item:
            item = pywikibot.ItemPage(importer.wikibase_repo, found_item)
        else:
            item = pywikibot.ItemPage(importer.wikibase_repo)

        def edit_diff(new_values, item_attr_obj, info_mode):
            diff = importer.get_diff_data(new_values, item_attr_obj)
            if diff:
                importer.edit_wikibase_item_info(info_mode, diff, wikibase_item=item)

        # Label
        new_labels = {}
        c_name = None
        if item_example.name and pd.notnull(item_example.name):
            # c_name = jaconv.zenkaku2hankaku(item_example.name, ascii=True)
            c_name = item_example.name
            new_labels["ja"] = c_name
        if item_example.enName and pd.notnull(item_example.enName):
            new_labels["en"] = item_example.enName
        if new_labels:
            edit_diff(new_labels, item.labels, cf.ItemAttribute.LABELS)

        # Aliases
        new_aliases = {}
        if item_example.furigana and pd.notnull(item_example.furigana):
            new_aliases["ja"] = item_example.furigana
            edit_diff(new_aliases, item.aliases, cf.ItemAttribute.ALIASES)

        # Claims
        def edit_claims(relation, value, lang="ja", message=""):
            try:
                claim = check_claim(item, relation, value, lang=lang)
                if claim:
                    debug = 1
                    if not check_source(claim, source_data):
                        create_source_claim(importer.wikibase_repo, claim, source_data)
                else:
                    claim = set_claim(
                        importer.wikibase_repo,
                        item,
                        relation,
                        value,
                        message,
                        lang=lang,
                    )
                    create_source_claim(importer.wikibase_repo, claim, source_data)
                return claim
            except Exception as message:
                iw.print_status(message)
                return None

        # Corporate Number (Japan) (P7) <-- P3225
        if item_example.corporateNumber and pd.notnull(item_example.corporateNumber):
            edit_claims("P7", item_example.corporateNumber, "corporateNumber")

        # P3057
        if item_example.successorCorporateNumber and pd.notnull(
            item_example.successorCorporateNumber
        ):
            edit_claims(
                "P3057",
                item_example.successorCorporateNumber,
                "successorCorporateNumber",
            )

        if c_name:
            # name in native language (P350) <-- P1559
            edit_claims("P350", c_name, lang="ja", message="name")

            # official name (P331) <-- P1448
            edit_claims("P331", c_name, lang="ja", message="name")

        if item_example.furigana and pd.notnull(item_example.furigana):
            # name in kana (P383) <-- P1814
            edit_claims("P383", item_example.furigana, "furigana")

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
        if item_example.kind and pd.notnull(item_example.kind):
            if item_example.kind == "101":
                edit_claims("P5", "Q396", "type")
            elif item_example.kind == "201":
                edit_claims("P5", "Q673", "type")
            elif item_example.kind == "301":
                edit_claims("P5", "Q859", "type")
            elif item_example.kind == "302":
                edit_claims("P5", "Q16924", "type")
            elif item_example.kind == "303":
                edit_claims("P5", "Q64297", "type")
            elif item_example.kind == "304":
                edit_claims("P5", "Q53926", "type")
            elif item_example.kind == "305":
                edit_claims("P5", "Q50699", "type")
            edit_claims("P5", "Q1867", "type")

        # inception (P164) <-- P571
        # if item_example.assignmentDate") and pd.notnull(
        #     item_example.assignmentDate
        # ):
        #     edit_claims("P164", item_example.assignmentDate, "assignmentDate

        # date of official closure (P1644) <-- P576
        if (
            item_example.closeDate
            and pd.notnull(item_example.closeDate)
            and item_example.closeCause
            and pd.notnull(item_example.closeCause)
            and item_example.closeCause in ["01", "11", "31"]
        ):
            edit_claims("P1644", item_example.closeDate, "closeDate")

        def add_qualifier(
            claim, prop="P48", value=None, is_item=True, lang="ja", message=""
        ):
            wb_prop = pywikibot.PropertyPage(importer.wikibase_repo, prop)
            if wb_prop.type == "monolingualtext":
                wb_value = pywikibot.WbMonolingualText(text=value, language=lang)
            elif wb_prop.type == "wikibase-item":
                wb_value = None
                if is_item:
                    responds = get_items(importer.wikibase_site, value, lang=lang)
                    if responds["search"]:
                        wb_value = pywikibot.ItemPage(
                            importer.wikibase_site, responds["search"][0]["id"]
                        )
            elif isinstance(value, Timestamp):
                wb_value = pywikibot.WbTime(
                    year=value.year, month=value.month, day=value.day
                )
            else:
                wb_value = value

            # check qualifier
            def is_add():
                if prop not in claim.qualifiers:
                    return True
                for qual in claim.qualifiers[prop]:
                    qual = qual.getTarget()
                    if qual == wb_value:
                        return False
                return True

            if is_add():
                qualifier = pywikibot.Claim(importer.wikibase_repo, prop)
                qualifier.setTarget(wb_value)

                try:
                    claim.addQualifier(qualifier, summary=f"Added {message}")
                except Exception as message:
                    iw.print_status(message)

        # headquarters location (P6) - City
        headquarters_claim = None
        if item_example.cityName and pd.notnull(item_example.cityName):
            search_results = get_items(
                importer.wikibase_site, item_example.cityName, lang="ja"
            )
            if search_results["search"]:
                claim = edit_claims(
                    "P6", search_results["search"][0]["id"], "prefecture"
                )
                if claim:
                    headquarters_claim = claim

        # postal code (P103) <-- P159 - P281
        if (
            headquarters_claim
            and item_example.prefectureName
            and pd.notnull(item_example.prefectureName)
        ):
            add_qualifier(
                headquarters_claim,
                prop="P48",
                is_item=True,
                value=item_example.prefectureName,
                message="prefectureName",
            )

        # postal code (P103) <-- P159 - P281
        if (
            headquarters_claim
            and item_example.postCode
            and pd.notnull(item_example.postCode)
        ):
            add_qualifier(
                headquarters_claim,
                prop="P103",
                is_item=False,
                value=item_example.postCode,
                message="postCode",
            )

        # headquarters location (P6) - street address (P814) <-- P159 - P6375
        if (
            headquarters_claim
            and item_example.streetNumber
            and pd.notnull(item_example.streetNumber)
        ):
            add_qualifier(
                headquarters_claim,
                prop="P814",
                is_item=False,
                value=item_example.streetNumber,
                message="streetNumber",
            )
        # headquarters location (P6) - street address (P814) <-- P159 - P6375
        if (
            headquarters_claim
            and item_example.enCityName
            and pd.notnull(item_example.enCityName)
        ):
            add_qualifier(
                headquarters_claim,
                prop="P814",
                is_item=False,
                value=item_example.enCityName,
                lang="en",
                message="enCityName",
            )

        # if item_example.addressOutside and pd.notnull(
        #     item_example.addressOutside
        # ):
        #     add_qualifier(
        #         prop1="48",
        #         prop="P814",
        #         is_item=False,
        #         value=item_example.addressOutside,
        #         message="addressOutside",
        #     )
        # if item_example.enAddressOutside and pd.notnull(
        #     item_example.enAddressOutside
        # ):
        #     add_qualifier(
        #         prop1="48",
        #         prop="P814",
        #         is_item=False,
        #         value=item_example.enAddressOutside,
        #         message="enAddressOutside",
        #     )
        p_bar.update()
        p_bar.set_description(desc=update_desc(f"Imported {item.id}"))
        # if item_i > 20:
        #     break


if __name__ == "__main__":
    m_f.init(is_log=True)
    params = sys.argv[1:]
    if len(params) == 1:
        from_i = params[0]
        to_i = -1
    elif len(params) == 2:
        from_i, to_i = params
    else:
        raise ValueError
    # print(from_i, type(from_i), to_i, type(to_i))
    test_import(from_i=int(from_i), to_i=int(to_i))  # , to_i=5000000
