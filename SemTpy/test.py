import semtui

semtui.SEMTUI_URI = "http://localhost:3002/api/"

table = semtui.getTable(29,253)


table = semtui.reconcile(table, 'citta', 'wikidata')

print(table)

semtui.updateTable(table)


table = semtui.getTable(29,253)

table = semtui.extendColumn(table, "citta", "wikidataGeoPropertiesSPARQL", ["wdt:P421", "wdt:P625"], ["time_zone","geocoordinates"])

semtui.updateTable(table)
#table = semtui.getTable(29,246)
#print(table['raw'])

#print(semtui.getDatasetsList())