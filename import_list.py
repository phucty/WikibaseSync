# configuration for pywikibot
import configparser
import sys
import os
import pywikibot
from pywikibot import config2
from tqdm import tqdm
import logging
from datetime import datetime
from utilities.util import WikibaseImporter
import others_config as cf


app_config = configparser.ConfigParser()
app_config.read("config/application.config.ini")
family = "my"
mylang = "my"
familyfile = os.path.relpath("./config/my_family.py")
if not os.path.isfile(familyfile):
    print("family file %s is missing" % (familyfile))
config2.register_family_file(family, familyfile)
config2.password_file = "user-password.py"
config2.usernames["my"]["my"] = app_config.get("wikibase", "user")

# Wikibase
wikibase_site = pywikibot.Site("my", "my")
# Wikidata
wikidata_site = pywikibot.Site("wikidata", "wikidata")

importer = WikibaseImporter(wikibase_site, wikidata_site)


def load_list(dir_file):
    with open(dir_file) as fp:
        line = fp.readline()
        while line:
            if line.startswith("#"):
                line = fp.readline()
                continue
            yield line
            line = fp.readline()


def import_wikidata_items(list_items, from_i=0, import_claims=True):
    for i, wikidata_id in enumerate(
        tqdm(list_items[from_i:], total=len(list_items), initial=from_i)
    ):
        importer.change_item(
            importer.wikidata_repo,
            importer.wikibase_repo,
            wikidata_id,
            statements=import_claims,
        )


init_list = cf.WC_INIT_ITEMS
import_wikidata_items(init_list, import_claims=True)

# full_list = list(load_list(dir_file="data/id.csv"))
# import_wikidata_items(full_list, import_claims=True)
