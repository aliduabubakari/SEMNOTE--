import requests
import json
import utils

SEMTUI_URI = ""

def getDatasetsList():
    """
    Show the datasets available in the backend

    :return: dataframe containing dataset information
    """
    response = requests.get(SEMTUI_URI+'/dataset/')
    return utils.cleanDatasetsData(response.text)

def getDataset(idDataset):
    """
    Show general information about the dataset

    :idDataset: the dataset's ID in the backend
    :return: dataframe containing general information about the dataset
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset))
    return utils.cleanDatasetsData(response.text)


def getDatasetTables(idDataset):
    """
    Show a list of tables contained in the dataset

    :idDataset: the dataset's ID in the backend
    :return: dataframe containing the list of tables and their respective information
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset) + '/table')
    return utils.cleanDatasetsTables(response.text)

def getTable(idDataset, idTable):
    """
    Retrieve a table from the backend in two different formats:
        - raw: the table in JSON format
        - parsed: the table in dataframe format

    :idDataset: the dataset's ID in the backend
    :idTable: the ID of the table to retrieve
    :return: the table in the two described formats
    """
    response = requests.get(SEMTUI_URI + 'dataset/' + str(idDataset)+'/table/'+str(idTable))
    return {'raw': json.loads(response.text)}

def getExtendersList():
    """
    Provides a list of available extenders with their main information

    :return: a dataframe containing extenders and their information
    """
    response = getExtenderData()
    return utils.cleanServiceList(response)

def getReconciliatorsList():
    """
    Provides a list of available reconciliators with their main information

    :return: a dataframe containing reconciliators and their information
    """
    response = getReconciliatorData()
    return utils.cleanServiceList(response)

def addTable(idDataset, filePath, tableName):
    """
    Allows adding a table to a dataset in the backend

    :idDataset: the dataset's ID in the backend
    :filePath: .csv file to be loaded as a table
    :tableName: the name the table will have in the dataset
    :return: loading status
    """
    url = SEMTUI_URI + 'dataset/' + str(idDataset) + '/table'
    files = {'file': open(filePath, 'rb')}
    response = requests.post(url, files=files, data={'name': tableName})
    return response.status_code

def reconcile(table, columnName, idReconciliator):
    """
    Reconciles a column with the chosen reconciliator

    :table: the table with the column to reconcile 
    :columnName: the name of the column to reconcile 
    :idReconciliator: ID of the reconciliator to use 
    :return: table with reconciled column
    """
    table = table['raw']
    reconciliatorResponse = getReconciliatorData()
    # creating the request
    url = SEMTUI_URI + '/reconciliators' + str(utils.getReconciliator(idReconciliator, reconciliatorResponse)['relativeUrl'])
    payload = utils.createReconciliationPayload(table, columnName, idReconciliator)
    response = requests.post(url, json=payload)
    response = json.loads(response.text)
    # inserting data into the table
    metadata = utils.createCellMetadataNameField(response, idReconciliator, reconciliatorResponse)
    table = utils.updateMetadataCells(table, metadata)
    table = utils.updateMetadataColumn(table, columnName, idReconciliator, metadata, reconciliatorResponse)
    table = utils.updateMetadataTable(table)
    return {'raw': table}

def updateTable(table):
    """
    Allows updating the table in the backend by inserting the table with the new information

    :table: the table to be updated within the backend
    :return: update status
    """
    table = table['raw']
    url = SEMTUI_URI + 'dataset/' + str(table["table"]["id"])+'/table/'+str(table["table"]["idDataset"])
    payload = utils.createUpdatePayload(table)
    response = requests.put(url, json=payload)
    return response.text

def extendColumn(table, reconciliatedColumnName, idExtender, properties, newColumnsName):
    """
    Allows extending specified properties present in the Knowledge Graph as a new column

    :table: the table containing the data
    :reconciliatedColumnName: the column containing the ID in the KG
    :idExtender: the extender to use for extension
    :properties: the properties to extend in the table
    :newColumnsName: the name of the new column to add
    :return: the extended table
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
    Retrieves extender data from the backend

    :return: data of extension services in JSON format
    """
    response = requests.get(SEMTUI_URI + '/extenders/list')
    return json.loads(response.text)

def getReconciliatorData():
    """
    Retrieves reconciliator data from the backend

    :return: data of reconciliator services in JSON format
    """
    response = requests.get(SEMTUI_URI + '/reconciliators/list')
    return json.loads(response.text)

