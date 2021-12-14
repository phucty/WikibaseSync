# configuration for pywikibot
import os
import pywikibot
from pywikibot import config2

import logging

from utilities.get_wikidata_changes import get_wikidata_changes

logging.getLogger("pywiki").disabled

from utilities.mapper import MapperID
from utilities.PropertyWikidataIdentifier import PropertyWikidataIdentifier
import configparser

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

# connect to the wikibase
wikibase = pywikibot.Site("my", "my")
wikibase_repo = wikibase.data_repository()
wikibase_repo.login()

# connect to wikidata
wikidata = pywikibot.Site("wikidata", "wikidata")
wikidata_repo = wikidata.data_repository()
wikibase_repo.login()

from utilities.util import changeItem

identifier = PropertyWikidataIdentifier()
identifier.get(wikibase_repo)
print("Wikidata Item Identifier", identifier.itemIdentifier)

idSparql = MapperID(
    app_config.get("wikibase", "sparqlEndPoint"),
    identifier.itemIdentifier,
    identifier.propertyIdentifier,
)
idSparql.load()

# grab all entities that changed
recent = get_wikidata_changes(None, 15)
for rc in recent:
    print(str(rc["title"]))
    if idSparql.contains_id(str(rc["title"])):
        print(
            "This entity ...",
            idSparql.get_id(str(rc["title"])),
            " corresponding to Wikidata entity "
            + str(rc["title"])
            + " has changed and will be sync!",
        )
        wikidata_item = pywikibot.ItemPage(wikidata_repo, str(rc["title"]))
        # check if the entity has some statements
        wikibase_item = pywikibot.ItemPage(
            wikibase_repo, idSparql.get_id(str(rc["title"]))
        )
        wikibase_item.get()
        count = 0
        for wikibase_claims in wikibase_item.claims:
            for wikibase_c in wikibase_item.claims.get(wikibase_claims):
                count = count + 1
        if count > 1:
            changeItem(wikidata_item, wikibase_repo, True)
        else:
            print("Change only the labels")
            changeItem(wikidata_item, wikibase_repo, False)
