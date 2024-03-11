import json
import pandas as pd
from datetime import datetime


# DATASET FUNCTIONS

def cleanDatasetsData(datasetsList):
    """
    permette di ripulire e formattare i dati riguardanti i dataset

    :datasetsList: dati riguardanti i datasets
    :return: un dataframe conentente le info dei datasets
    """
    datasetsList = json.loads(datasetsList)
    datasets = pd.DataFrame(
        columns=["id", "name", "nTables", "lastModifiedDate"])
    if "collection" not in datasetsList:
        datasetsList = {'collection': [datasetsList]}
    for dataset in datasetsList["collection"]:
        datasets.loc[len(datasets)] = [dataset["id"], dataset["name"],
                                       dataset["nTables"], dataset["lastModifiedDate"]]
    return datasets

# DATASET_TABLE


def cleanDatasetsTables(tableList):
    """
    permette di ripulire e formattare i dati delle tabelle 
    contenute in un dataset

    :tableList: dati riguardanti le tabelle di un dataset
    :return: un dataframe conentente le info delle tabelle
    """
    tableList = json.loads(tableList)
    tables = pd.DataFrame(
        columns=["id", "idDataset", "name", "nCols", "nRows", "lastModifiedDate"])
    for table in tableList["collection"]:
        tables.loc[len(tables)] = [table["id"], table["idDataset"], table["name"],
                                   table["nCols"], table["nRows"], table["lastModifiedDate"]]
    return tables

# SERVICE FUNCTIONS


def cleanServiceList(serviceList):
    """
    permette di ripulire e formattare la lista dei servizi

    :serviceList: dati riguardanti i servizi disponibili
    :return: dataframe contenente le info dei riconciliatori
    """
    serviceList = serviceList
    reconciliators = pd.DataFrame(columns=["id", "relativeUrl", "name"])
    for reconciliator in serviceList:
        reconciliators.loc[len(reconciliators)] = [
            reconciliator["id"], reconciliator["relativeUrl"], reconciliator["name"]]
    return reconciliators

# EXTENDER FUNCTIONS


def getExtender(idExtender, response):
    """
    passando l'id dell'extender ritorna le principali info in formato json

    :idExtender: id dell'extender in questione
    :response: json contenente le informazioni degli extenders
    :return: json contente le principali info dell'extender
    """
    for extender in response:
        if extender['id'] == idExtender:
            return {
                'name': extender['name'],
                'relativeUrl': extender['relativeUrl']
            }
    return None

# RECONCILATOR FUNCTIONS


def getReconciliator(idReconciliator, response):
    """
    funzione che dato l'id del riconciliatore restituisce 
    un dizionario con tutte le informazioni del servizio 

    :idReconciliator: id del riconciliatore in questione
    :return: un dizionario con le informazioni del riconciliatore
    """
    for reconciliator in response:
        if reconciliator['id'] == idReconciliator:
            return {
                'uri': reconciliator['uri'],
                'prefix': reconciliator['prefix'],
                'name': reconciliator['name'],
                'relativeUrl': reconciliator['relativeUrl']
            }
    return None


def getReconciliatorFromPrefix(prefixReconciliator, response):
    """
    funzione che dato l'id del riconciliatore restituisce 
    un dizionario con tutte le informazioni del servizio 

    :idReconciliator: id del riconciliatore in questione
    :return: un dizionario con le informazioni del riconciliatore
    """
    for reconciliator in response:
        if reconciliator['prefix'] == prefixReconciliator:
            return {
                'uri': reconciliator['uri'],
                'id': reconciliator['id'],
                'name': reconciliator['name'],
                'relativeUrl': reconciliator['relativeUrl']
            }
    return None


# CELL OPERATIONS

def calculateScoreBoundCell(metadata):
    """
    calcola il valore min e max dello score dei risultati ottenuti per
    una singola cella

    :metadata: metadata di una singola cella
    :return: un dizionario contenente i due valori
    """
    try:
        scoreList = [item['score'] for item in metadata]
        return {'lowestScore': min(scoreList), 'highestScore': max(scoreList)}
    except:
        return {'lowestScore': 0, 'highestScore': 0}


def createAnnotationMetaCell(metadata):
    """
    crea il campo annotationMeta a livello di celle, 
    che poi verrà inserito in tabella

    :metadata: metadata a livello di cella
    :return: il dizionario con i dati riguardanti annotationMeta
    """
    scoreBound = calculateScoreBoundCell(metadata)
    return {'annotated': True,
            'match': {'value': valueMatchCell(metadata)},
            'lowestScore': scoreBound['lowestScore'],
            'highestScore': scoreBound['highestScore']}


def valueMatchCell(metadata):
    """
    restituisce se una cella ha ottenuto un match oppure no

    :metadata: metadata a livello di cella
    :return: True o False in base all'avvenuto match
    """
    for item in metadata:
        if item['match'] == True:
            return True
    return False


def createCellMetadataNameField(metadata, idReconciliator, reconciliatorResponse):
    """
    refactor del campo name all'interno dei metadata a livello di cella
    necessario per la visualizzazione all'interno di semtui

    :metadata: metadata a livello di colonna
    :idReconciliator: id del riconciliatore effettuato nell'operazione 
    :columnName: nome della colonna su cui si sta lavorando
    :return: metadata contenente il campo name nel nuovo formato
    """
    for row in range(len(metadata)):
        try:
            for item in range(len(metadata[row]["metadata"])):
                value = metadata[row]["metadata"][item]['name']
                uri = metadata[row]["metadata"][item]['id']
                metadata[row]["metadata"][item]['name'] = parseNameField(value, getReconciliator(
                    idReconciliator, reconciliatorResponse)['uri'], uri.split(':')[1])
        except:
            return []
    return metadata


def updateMetadataCells(table, metadata):
    """
    permette di inserire i nuovi metadata a livello di celle

    :table: tabella in formato raw
    :metadata: metadata a livello di cella
    :return: la tabella in formato raw con i metadata
    """
    for item in metadata:
        item["id"] = item["id"].split("$")
        try:
            table["rows"][item["id"][0]]["cells"][item["id"]
                                                  [1]]["metadata"] = item["metadata"]
            table["rows"][item["id"][0]]["cells"][item["id"][1]
                                                  ]["annotationMeta"] = createAnnotationMetaCell(item["metadata"])
        except:
            print("")
    return table


def addExtendedCell(table, newColumnData, newColumnName, idReconciliator, reconciliatorResponse):
    """
    crea ed inserisce all'interno della tabella (a livello di cella) i dati relativi alla nuova colonna 
    aggiunta con l'estensione

    :table: table in formato raw
    :newColumnData: i dati ottenuti dall'extender
    :newColumnName: il nome della nuova colonna da aggiungere in tabella
    :idReconciliator: l'id del riconciliatore utilizzato nella colonna di partenza
    :return: la tabella con i campi cell completati
    """
    rowKeys = newColumnData['cells']
    entity = checkEntity(newColumnData)
    if 'kind' in newColumnData: 
        columnType = newColumnData['kind']
    else:
        if entity == True:
            columnType = 'entity'
        else:
            columnType = 'literal'
    print(columnType)
    for rowKey in rowKeys:
        table['rows'][rowKey]['cells'][newColumnName] = {}
        table['rows'][rowKey]['cells'][newColumnName]['id'] = str(
            rowKey) + "$" + str(newColumnName)
        table['rows'][rowKey]['cells'][newColumnName]['label'] = newColumnData['cells'][rowKey]['label']
        table['rows'][rowKey]['cells'][newColumnName]['metadata'] = parseNameEntities(
            newColumnData['cells'][rowKey]['metadata'], getReconciliator(idReconciliator, reconciliatorResponse)['uri'])
        if columnType == 'entity':
            table['rows'][rowKey]['cells'][newColumnName]['annotationMeta'] = createAnnotationMetaCell(
                table['rows'][rowKey]['cells'][newColumnName]['metadata'])
        else:
            table['rows'][rowKey]['cells'][newColumnName]['annotationMeta'] = {}
    return table


# COLUMN OPERATIONS

def calculateScoreBoundColumn(table, columnName, reconciliatorResponse):
    """
    calcola il valore min e max dello score dei risultati ottenuti per
    una singola colonna, inoltre restituisce se tutte le celle hanno ottenuto un match
    o meno

    :table: la tabella in formato raw
    :columnName: il nome della colonna su cui lavorare
    :return: un dizionario contenente i risultati
    """
    allScores = []
    matchValue = True
    rows = table["rows"].keys()
    for row in rows:
        try:
            annotationMeta = table["rows"][row]['cells'][columnName]['annotationMeta']
            if annotationMeta['annotated'] == True:
                allScores.append(annotationMeta['lowestScore'])
                allScores.append(annotationMeta['highestScore'])
            if annotationMeta['match']['value'] == False:
                matchValue = False
        except:
            print("Missed cell annotation metadata")
    try:
        return {'lowestScore': min(allScores), 'highestScore': max(allScores), 'matchValue': matchValue}
    except:
        return {'lowestScore': 0, 'highestScore': 0, 'matchValue': False}


def calculateNCellsReconciliatedColumn(table, columnName):
    """
    calcola il numero di celle riconciliate all'interno di 
    una colonna

    :table: tabella in formato raw
    :columnName: nome della colonna in questione
    :return: il numero di celle riconciliate
    """
    cellsReconciliated = 0
    rowsIndex = table["rows"].keys()
    for row in rowsIndex:
        try:
            if table['rows'][row]['cells'][columnName]['annotationMeta']["annotated"] == True:
                cellsReconciliated += 1
        except:
            cellsReconciliated = cellsReconciliated
    return cellsReconciliated


def updateMetadataColumn(table, columnName, idReconciliator, metadata, reconciliatorResponse):
    """
    permette di inserire i metadata a livello di colonna

    :table: tabella in formato raw
    :columnName: nome della colonna su cui operare
    :reconciliatorId: id del riconciliatore utilizzato
    :metadata: metadata a livello di colonna
    :return: la tabella con i nuovi metadata inseriti
    """
    # chiedere i diversi stati
    table['columns'][columnName]['status'] = 'pending'
    table['columns'][columnName]['kind'] = "entity"
    table['columns'][columnName]['context'] = createContextColumn(
        table, columnName, idReconciliator, reconciliatorResponse)
    table['columns'][columnName]['metadata'] = createMetadataFieldColumn(
        metadata)
    table['columns'][columnName]['annotationMeta'] = createAnnotationMetaColumn(
        True, table, columnName, reconciliatorResponse)
    return table


def createMetadataFieldColumn(metadata):
    """
    permette di creare il campo metadata per una colonna, che si
    inserirà poi nei metadata generali a livello di colonna

    :metadata: metadata a livello di colonna
    :return: il campo metadata a livello di colonna
    """
    return [
        {'id': '',
         'match': getColumnMetadata(metadata)['matchMetadataValue'],
         'score': 0,
         'name':{'value': '', 'uri': ''},
         'entity': getColumnMetadata(metadata)['entity'],
         'property':[],
         'type': getColumnMetadata(metadata)['type']}
    ]


def createAnnotationMetaColumn(annotated, table, columnName, reconciliatorResponse):
    scoreBound = calculateScoreBoundColumn(
        table, columnName, reconciliatorResponse)
    return {'annotated': annotated,
            'match': {'value': scoreBound['matchValue']},
            'lowestScore': scoreBound['lowestScore'],
            'highestScore': scoreBound['highestScore']
            }


def getColumnMetadata(metadata):
    """
    permette di recuperare dati a livello di colonna, in particolare
    l'entità corrispondenti alla colonna, i tipi della colonna,
    e il valore match delle entità della colonna

    :metadata: metadata della colonna ottenuti dal riconciliatore
    :return: dizionario contenente i diversi dati
    """
    entity = []
    types = []
    for i in range(len(metadata)):
        try:
            if metadata[i]['id'] == ['column', 'index']:
                entity = metadata[i]['metadata']
        except:
            print("No column entity is provided")
        try:
            if metadata[i]['id'] != ['column', 'index']:
                for j in range(len(metadata[i]['metadata'])):
                    if metadata[i]['metadata'][j]['match'] == True:
                        types.append(metadata[i]['metadata'][j]['type'][0])
        except:
            print("No column type is provided")
    matchMetadataValue = True
    for item in entity:
        if item['match'] == False:
            matchMetadataValue = False
    return {'entity': entity, 'type': types, 'matchMetadataValue': matchMetadataValue}


def createContextColumn(table, columnName, idReconciliator, reconciliatorResponse):
    """
    crea il campo context a livello di colonna recuperando i dati necessari

    :table: table in formato raw
    :columnName: il nome della colonna di cui si sta creando il contesto
    :idReconciliator: l'id del riconciliatore utilizzato per la colonna
    :return: il campo context della colonna
    """
    nCells = len(table["rows"].keys())
    reconciliator = getReconciliator(idReconciliator, reconciliatorResponse)
    return {reconciliator['prefix']: {
            'uri': reconciliator['uri'],
            'total': nCells,
            'reconciliated': calculateNCellsReconciliatedColumn(table, columnName)
            }}

def checkEntity(newColumnData):
    righe = newColumnData['cells'].keys()
    entity = False
    for riga in righe:
        if 'metadata' in newColumnData['cells'][riga] and newColumnData['cells'][riga]['metadata'] != []:
            entity = True
    return entity

def addExtendedColumn(table, newColumnData, newColumnName, idReconciliator, reconciliatorResponse):
    """
    crea ed inserisce all'interno della tabella (a livello di colonna) i dati relativi alla nuova colonna 
    aggiunta con l'estensione

    :table: table in formato raw
    :newColumnData: i dati ottenuti dall'extender
    :newColumnName: il nome della nuova colonna da aggiungere in tabella
    :idReconciliator: l'id del riconciliatore utilizzato nella colonna di partenza
    :return: la tabella con i campi column completati
    """
    entity = checkEntity(newColumnData)
    print(entity)
    table['columns'][newColumnName] = {}
    table['columns'][newColumnName]['id'] = newColumnName
    table['columns'][newColumnName]['label'] = newColumnName
    table['columns'][newColumnName]['status'] = 'extended'
    if 'kind' in newColumnData:
        table['columns'][newColumnName]['kind'] = newColumnData['kind']
    else:
        if entity == True:
            table['columns'][newColumnName]['kind'] = 'entity'
        table['columns'][newColumnName]['kind'] = 'literal'
    table['columns'][newColumnName]['metadata'] = parseNameMetadata(
        newColumnData['metadata'], getReconciliator(idReconciliator, reconciliatorResponse)['uri'])

    if ('kind' in newColumnData and newColumnData['kind'] == 'entity') or entity == True:
        table['columns'][newColumnName]['annotationMeta'] = createAnnotationMetaColumn(
            True, table, newColumnName, reconciliatorResponse)
        table['columns'][newColumnName]['context'] = createContextColumn(
            table, newColumnName, idReconciliator, reconciliatorResponse)
    else:
        table['columns'][newColumnName]['annotationMeta'] = {}
        table['columns'][newColumnName]['context'] = {}
    return table


def addExtendedColumns(table, extensionData, newColumnsName, reconciliatorResponse):
    """
    permette di iterare le operazioni di inserimento di una singola colonna per
    tutte le proprietà da inserire 

    :table: table in formato raw
    :extensionData: i dati ottenuti dall'extender
    :newColumnsName: i nomi delle nuove colonne da inserire nella tabella
    :return: la tabella con i nuovi campi inseriti
    """
    newColumns = extensionData['columns'].keys()
    i = 0
    for columnKey in newColumns:
        idReconciliator = getColumnIdReconciliator(
            table, extensionData['meta'][columnKey], reconciliatorResponse)
        table = addExtendedCell(
            table, extensionData['columns'][columnKey], newColumnsName[i], idReconciliator, reconciliatorResponse)
        table = addExtendedColumn(
            table, extensionData['columns'][columnKey], newColumnsName[i], idReconciliator, reconciliatorResponse)
        i += 1
    return table


def getColumnIdReconciliator(table, columnName, reconciliatorResponse):
    """
    specficando la colonna di interesse ritorna l'id del riconciliatore, 
    se la colonna è riconcliata

    :table: tabella in formato raw
    :columnName: nome della colonna in questione
    :return: id del riconciliatore utilizzato
    """
    prefix = list(table['columns'][columnName]['context'].keys())
    return getReconciliatorFromPrefix(prefix[0], reconciliatorResponse)['id']


# TABLE OPERATIONS

def calculateScoreBoundTable(table):
    """
    calcola il valore min e max dello score ottenuto nei
    risultati di tutta la tabella

    :table: la tabella in formato raw
    :return: un dizionario contenente i due valori
    """
    allScores = []
    reconciliateColumns = [column for column in table['columns'].keys(
    ) if table['columns'][column]['status'] != 'empty']
    for column in reconciliateColumns:
        try:
            if table['columns'][column]['annotationMeta']['annotated'] == True:
                allScores.append(table['columns'][column]
                                 ['annotationMeta']['lowestScore'])
                allScores.append(table['columns'][column]
                                 ['annotationMeta']['highestScore'])
        except:
            print("Missed column annotation metadata")
    try:
        return {'lowestScore': min(allScores), 'highestScore': max(allScores)}
    except:
        return {'lowestScore': 0, 'highestScore': 0}


def calculateNCellsReconciliated(table):
    """
    calcola il numero di celle riconciliate all'interno della
    tabella

    :table: la tabella in formato raw
    :return: il numero di celle riconciliate
    """
    cellsReconciliated = 0
    columnsName = table['columns'].keys()
    for column in columnsName:
        try:
            contextReconciliator = table['columns'][column]['context'].keys()
            for reconcliator in contextReconciliator:
                cellsReconciliated += int(table['columns'][column]
                                          ['context'][reconcliator]['reconciliated'])
        except:
            cellsReconciliated += 0
    return cellsReconciliated


def updateMetadataTable(table):
    """
    permette di inserire i metadata a livello di tabella

    :table: tabella in formato raw
    :return: la tabella con i nuovi metadata inseriti
    """
    scoreBound = calculateScoreBoundTable(table)
    table['table']['minMetaScore'] = scoreBound['lowestScore']
    table['table']['maxMetaScore'] = scoreBound['highestScore']
    table['table']['nCellsReconciliated'] = calculateNCellsReconciliated(table)
    return table


# PAYLOAD

def createUpdatePayload(table):
    """
    permette di creare il payload necessario a compiere l'operazione
    di update della tabella

    :table: table in formato raw
    :return: payload della richiesta
    """
    payload = {"tableInstance": {}, "columns": {}, "rows": {}}
    payload["tableInstance"] = {'id': table["table"]["id"],
                                'idDataset': table["table"]["idDataset"],
                                'name': table["table"]["name"],
                                'nCols': table["table"]["nCols"],
                                'nRows': table["table"]["nRows"],
                                'nCells': table["table"]["nCells"],
                                'nCellsReconciliated': table["table"]["nCellsReconciliated"],
                                'lastModifiedDate': datetime.now().strftime("%Y/%m/%dT%H:%M:%SZ")
                                }
    payload["columns"]["allIds"] = list(table["columns"].keys())
    payload["rows"]["allIds"] = list(table["rows"].keys())
    payload["rows"]["byId"] = table["rows"]
    payload["columns"]["byId"] = table["columns"]
    return payload


def createReconciliationPayload(table, columnName, idReconciliator):
    """
    crea il payload per la richiesta di riconciliazione

    :table: table in formato raw
    :columnName: il nome della colonna da riconciliare
    :idReconciliator: l'id del servizio di riconciliazione da utilizzare
    :return: payload della richiesta
    """
    rows = []
    rows.append({"id": 'column$index', "label": columnName})
    for row in table['rows'].keys():
        rows.append({"id": row+"$"+columnName,
                    "label": table['rows'][row]['cells'][columnName]['label']})
    return {"serviceId": idReconciliator, "items": rows}


def createExensionPayload(table, reconciliatedColumnName, idExtender, properties):
    """
    crea il payload per la richiesta di estensione 

    :table: table in formato raw
    :reconciliatedColumnName: il nome della colonna contenente id riconciliato
    :extenderId: l'id del servizio di riconciliazione da utilizzare
    :properties: le proprietà da utilizzare sottoforma di lista
    :return: payload della richiesta
    """
    items = {}
    rows = table['rows'].keys()
    for row in rows:
        cell = table['rows'][row]['cells'][reconciliatedColumnName]
        if cell['annotationMeta']['match']['value'] == True:
            for metadata in cell['metadata']:
                if metadata['match'] == True:
                    items[row] = metadata['id']
                    break
    payload = {"serviceId": idExtender,
               "items": {
                   str(reconciliatedColumnName): items
               },
               "property": properties
               }
    return payload


# PARSE FUNCTIONS

def parseNameMetadata(metadata, uriReconciliator):
    """
    converte nel giusto formato il nome da inserire nel campo metadata

    :metadata: metadati della cella/colonna
    :uriReconciliator: l'uri del KG di appartenenza
    :return: i metadata in formato corretto
    """
    for item in metadata:
        item['entity'] = parseNameEntities(item['entity'], uriReconciliator)
    return metadata


def parseNameEntities(entities, uriReconciliator):
    """
    funzione iterata in parseNameMetadata, lavora a livello di entità

    :entities: entità presenti nella cella/colonna
    :uriReconciliator: l'uri del KG di appartenenza
    :return: le entità in formato corretto
    """
    for entity in entities:
        entity['name'] = parseNameField(
            entity['name'], uriReconciliator, entity['id'].split(':')[1])
    return entities


def parseNameField(name, uriReconciliator, idEntity):
    """
    funzione vera e propria che cambia il formato del nome, in quello richiesto
    per la visualizzazione 

    :name: nome dell'entità
    :uriReconciliator: l'uri del KG di appartenenza
    :idEntity: id dell'entità
    :return: il nome in formato corretto
    """
    return {
        'value': name,
        'uri': uriReconciliator + idEntity
    }


def parseTable(table):
    """
    permette di ottenere la tabella in formato parsed, sottoforma di 
    dataframe

    :table: table in formato raw
    :return: un dataframe che rappresenta la tabella in formato parsed
    """
    # da completare
    table = json.loads(table)
    columnName = ["tableIndex"]
    columnName.extend(list(table["columns"].keys()))
    dfTable = pd.DataFrame(columns=columnName)
    for rowIndex in table["rows"].keys():
        row = []
        row.append(table["rows"][rowIndex]["id"])
        for columnName in list(table["columns"].keys()):
            try:
                row.append(table["rows"][rowIndex]
                           ["cells"][columnName]["label"])
            except:
                row.append(None)
        dfTable.loc[len(dfTable)] = row
    dfTable = dfTable.set_index(['tableIndex'])
    return dfTable
