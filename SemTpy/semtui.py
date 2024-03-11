import requests
import json
import utils

SEMTUI_URI = ""


def getDatasetsList():
    """
    mostra i dataset presenti nel backend

    :return: dataframe contenente i dataset con info
    """
    response = requests.get(SEMTUI_URI+'/dataset/')
    return utils.cleanDatasetsData(response.text)

def getDataset(idDataset):
    """
    mostra le info generali del dataset

    :idDataset: l'id del dataset contenuto nel backend
    :return: dataframe contenente le info generali del dataset
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset))
    return utils.cleanDatasetsData(response.text)

def getDatasetTables(idDataset):
    """
    mostra una lista delle tabelle contenute nel dataset

    :idDataset: l'id del dataset contenuto nel backend
    :return: dataframe contenente la lista delle tabelle e delle relative info
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset) + '/table')
    return utils.cleanDatasetsTables(response.text)

def getTable(idDataset, idTable):
    """
    recupera dal backend una tabella, in due differenti formati:
        -raw: la tabella in formato JSON
        -parsed: la tabella in forrmato dataframe

    :idDataset: l'id del dataset contenuto nel backend
    :idTable: l'id della tabella da recuperare
    :return: la tabella nei due formati descritti
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset)+'/table/'+str(idTable))
    return ({'raw': json.loads(response.text)})

def getExtendersList():
    """
    fornisce una lista degli extender disponibili con le info principali

    :return: un dataframe contenente gli extender e le info 
    """
    response = getExtenderData()
    return utils.cleanServiceList(response)

def getReconciliatorsList():
    """
    fornisce una lista dei riconciliatori disponibili con le info principali

    :return: un dataframe contenente i riconciliatori e le info
    """
    response = getReconciliatorData()
    return utils.cleanServiceList(response)

def addTable(idDataset, filePath, tableName):
    """
    permette di aggiungere una tabella ad un dataset presente nel backend

    :idDataset: l'id del dataset contenuto nel backend
    :filePath: file .csv da caricare come tabella 
    :tableName: nome che la tabella avrà nel dataset 
    :return: stato del caricamento
    """
    url = SEMTUI_URI + 'dataset/' + str(idDataset) + '/table'
    files = {'file': open(filePath, 'rb')}
    response = requests.post(url, files=files, data={'name': tableName})
    return response.status_code

def reconcile(table, columnName, idReconciliator):
    """
    riconcilia una colonna con il riconciliatore scelto

    :table: la tabella con la colonna da riconciliare 
    :columnName: il nome della colonna da riconcilare 
    :idReconcilator: id riconciliatore da utilizzare 
    :return: tabella con colonna riconciliata
    """
    table = table['raw']
    reconciliatorResponse = getReconciliatorData()
    #creazione della richiesta
    url = SEMTUI_URI + '/reconciliators' + str(utils.getReconciliator(idReconciliator, reconciliatorResponse)['relativeUrl'])
    payload = utils.createReconciliationPayload(table, columnName, idReconciliator)
    response = requests.post(url, json=payload)
    response = json.loads(response.text)
    #inserimento dati in tabella
    metadata = utils.createCellMetadataNameField(response, idReconciliator, reconciliatorResponse)
    table = utils.updateMetadataCells(table, metadata)
    table = utils.updateMetadataColumn(table, columnName, idReconciliator, metadata, reconciliatorResponse)
    table = utils.updateMetadataTable(table)
    return {'raw': table}

def updateTable(table):
    """
    permette di aggiornare la tabella nel backend inserendo la tabella con le nuove
    informazioni

    :table: la tabella da aggiornare all'interno del backend
    :return: stato dell'aggiornamento
    """
    table = table['raw']
    url = SEMTUI_URI + 'dataset/' + str(table["table"]["id"])+'/table/'+str(table["table"]["idDataset"])
    payload = utils.createUpdatePayload(table)
    response = requests.put(url, json=payload)
    return response.text

def extendColumn(table, reconciliatedColumnName, idExtender, properties, newColumnsName):
    """
    permette di estendere come nuova colonna le proprietà specificate, presenti nel
    Knowledge graph

    :table: la tabella contente i dati
    :reconciliatedColumnName: la colonna che contiene l'id nel KG
    :idExtender: l'extender da utilizzare per l'estensione
    :properties: le proprietà da estendere in tabella
    :newColumnsName: il nome della nuova colonna da aggiungere
    :return: la tabella estesa
    """
    reconciliatorResponse = getReconciliatorData()
    table = table["raw"]
    url = SEMTUI_URI + "extenders/" + \
        str(utils.getExtender(idExtender, getExtenderData())['relativeUrl'])
    payload = utils.createExensionPayload(table, reconciliatedColumnName, idExtender, properties)
    response = requests.post(url, json=payload)
    table = utils.addExtendedColumns(table, json.loads(response.text), newColumnsName, reconciliatorResponse)
    return {'raw': table}

def getExtenderData():
    """
    permette di recuperare i dati degli extender dal backend

    :return: i dati dei servizi di estesione in formato json
    """
    response = requests.get(SEMTUI_URI + '/extenders/list')
    return json.loads(response.text)

def getReconciliatorData():
    """
    permette di recuperare i dati dei riconciliatori dal backend

    :return: i dati dei servizi di riconciliatori in formato json
    """
    response = requests.get(SEMTUI_URI + '/reconciliators/list')
    return json.loads(response.text)
