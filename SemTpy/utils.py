import json
import pandas as pd
from datetime import datetime

# DATASET FUNCTIONS

def cleanDatasetsData(datasetsList):
    """
    Cleans and formats data related to datasets

    :datasetsList: data regarding datasets
    :return: a dataframe containing datasets information
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
    Cleans and formats data of tables contained in a dataset

    :tableList: data regarding tables of a dataset
    :return: a dataframe containing tables information
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
    Cleans and formats the service list

    :serviceList: data regarding available services
    :return: dataframe containing reconciliators information
    """
    serviceList = serviceList
    reconciliators = pd.DataFrame(columns=["id", "relativeUrl", "name"])
    for reconciliator in serviceList:
        reconciliators.loc[len(reconciliators)] = [
            reconciliator["id"], reconciliator["relativeUrl"], reconciliator["name"]]
    return reconciliators


def getExtender(idExtender, response):
    """
    Given the extender's ID, returns the main information in JSON format

    :idExtender: the ID of the extender in question
    :response: JSON containing information about the extenders
    :return: JSON containing the main information of the extender
    """
    for extender in response:
        if extender['id'] == idExtender:
            return {
                'name': extender['name'],
                'relativeUrl': extender['relativeUrl']
            }
    return None


# RECONCILIATOR FUNCTIONS

def getReconciliator(idReconciliator, response):
    """
    Function that, given the reconciliator's ID, returns a dictionary 
    with all the service information

    :idReconciliator: the ID of the reconciliator in question
    :return: a dictionary with the reconciliator's information
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
    Function that, given the reconciliator's prefix, returns a dictionary 
    with all the service information

    :prefixReconciliator: the prefix of the reconciliator in question
    :return: a dictionary with the reconciliator's information
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
    Calculates the min and max value of the score of the results obtained for
    a single cell

    :metadata: metadata of a single cell
    :return: a dictionary containing the two values
    """
    try:
        scoreList = [item['score'] for item in metadata]
        return {'lowestScore': min(scoreList), 'highestScore': max(scoreList)}
    except:
        return {'lowestScore': 0, 'highestScore': 0}


def createAnnotationMetaCell(metadata):
    """
    Creates the annotationMeta field at the cell level, 
    which will then be inserted into the table

    :metadata: cell-level metadata
    :return: the dictionary with data regarding annotationMeta
    """
    scoreBound = calculateScoreBoundCell(metadata)
    return {'annotated': True,
            'match': {'value': valueMatchCell(metadata)},
            'lowestScore': scoreBound['lowestScore'],
            'highestScore': scoreBound['highestScore']}


def valueMatchCell(metadata):
    """
    Returns whether a cell has obtained a match or not

    :metadata: cell-level metadata
    :return: True or False based on the match occurrence
    """
    for item in metadata:
        if item['match'] == True:
            return True
    return False


def createCellMetadataNameField(metadata, idReconciliator, reconciliatorResponse):
    """
    Refactor of the name field within cell-level metadata
    necessary for visualization within SEMTUI

    :metadata: column-level metadata
    :idReconciliator: ID of the reconciliator performed in the operation
    :reconciliatorResponse: response containing reconciliator information
    :return: metadata containing the name field in the new format
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
    Allows inserting new cell-level metadata

    :table: table in raw format
    :metadata: cell-level metadata
    :return: the table in raw format with metadata
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
    Creates and inserts data related to the new column added with the extension into the table 
    at the cell level.

    :table: table in raw format
    :newColumnData: data obtained from the extender
    :newColumnName: the name of the new column to add in the table
    :idReconciliator: the ID of the reconciliator used in the original column
    :reconciliatorResponse: response containing reconciliator information
    :return: the table with completed cell fields
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
    Calculates the min and max value of the score of the results obtained for
    a single column, also returns whether all cells obtained a match or not

    :table: the table in raw format
    :columnName: the name of the column to work on
    :reconciliatorResponse: response containing reconciliator information
    :return: a dictionary containing the results
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
    Calculates the number of reconciled cells within 
    a column

    :table: table in raw format
    :columnName: name of the column in question
    :return: the number of reconciled cells
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
    Allows inserting column-level metadata

    :table: table in raw format
    :columnName: name of the column to operate on
    :idReconciliator: ID of the reconciliator used
    :metadata: column-level metadata
    :reconciliatorResponse: response containing reconciliator information
    :return: the table with the new metadata inserted
    """
    # inquire about the different states
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
    Allows creating the metadata field for a column, which will
    then be inserted into the general column-level metadata

    :metadata: column-level metadata
    :return: the metadata field at the column level
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
    Allows retrieving column-level data, particularly
    the entity corresponding to the column, the column types,
    and the match value of the entities in the column

    :metadata: column metadata obtained from the reconciliator
    :return: dictionary containing the different data
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
    Creates the context field at the column level by retrieving the necessary data

    :table: table in raw format
    :columnName: the name of the column for which the context is being created
    :idReconciliator: the ID of the reconciliator used for the column
    :reconciliatorResponse: response containing reconciliator information
    :return: the context field of the column
    """
    nCells = len(table["rows"].keys())
    reconciliator = getReconciliator(idReconciliator, reconciliatorResponse)
    return {reconciliator['prefix']: {
            'uri': reconciliator['uri'],
            'total': nCells,
            'reconciliated': calculateNCellsReconciliatedColumn(table, columnName)
            }}

def checkEntity(newColumnData):
    rows = newColumnData['cells'].keys()
    entity = False
    for row in rows:
        if 'metadata' in newColumnData['cells'][row] and newColumnData['cells'][row]['metadata'] != []:
            entity = True
    return entity

def addExtendedColumn(table, newColumnData, newColumnName, idReconciliator, reconciliatorResponse):
    """
    Creates and inserts data into the table (at the column level) related to the new column 
    added with the extension

    :table: table in raw format
    :newColumnData: data obtained from the extender
    :newColumnName: the name of the new column to add in the table
    :idReconciliator: the ID of the reconciliator used in the original column
    :reconciliatorResponse: response containing reconciliator information
    :return: the table with completed column fields
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
    Allows iterating the operations to insert a single column for
    all the properties to be inserted

    :table: table in raw format
    :extensionData: data obtained from the extender
    :newColumnsName: names of the new columns to insert into the table
    :return: the table with the new fields inserted
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
    Specifying the column of interest returns the reconciliator's ID,
    if the column is reconciled

    :table: table in raw format
    :columnName: name of the column in question
    :return: the ID of the reconciliator used
    """
    prefix = list(table['columns'][columnName]['context'].keys())
    return getReconciliatorFromPrefix(prefix[0], reconciliatorResponse)['id']


# TABLE OPERATIONS

def calculateScoreBoundTable(table):
    """
    Calculates the minimum and maximum score obtained in
    the results of the entire table

    :table: the table in raw format
    :return: a dictionary containing the two values
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
    Calculates the number of reconciled cells within the
    table

    :table: the table in raw format
    :return: the number of reconciled cells
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
    Inserts metadata at the table level

    :table: table in raw format
    :return: the table with the new metadata inserted
    """
    scoreBound = calculateScoreBoundTable(table)
    table['table']['minMetaScore'] = scoreBound['lowestScore']
    table['table']['maxMetaScore'] = scoreBound['highestScore']
    table['table']['nCellsReconciliated'] = calculateNCellsReconciliated(table)
    return table


# PAYLOAD

def createUpdatePayload(table):
    """
    Creates the payload required to perform the table update operation

    :table: table in raw format
    :return: request payload
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
   

def createReconciliationPayload(table, columnName, idReconciliator):
    """
    Creates the payload for the reconciliation request

    :table: table in raw format
    :columnName: the name of the column to reconcile
    :idReconciliator: the id of the reconciliation service to use
    :return: the request payload
    """
    rows = []
    rows.append({"id": 'column$index', "label": columnName})
    for row in table['rows'].keys():
        rows.append({"id": row+"$"+columnName,
                    "label": table['rows'][row]['cells'][columnName]['label']})
    return {"serviceId": idReconciliator, "items": rows}


def createExensionPayload(table, reconciliatedColumnName, idExtender, properties):
    """
    Creates the payload for the extension request

    :table: table in raw format
    :reconciliatedColumnName: the name of the column containing reconciled id
    :idExtender: the id of the extension service to use
    :properties: the properties to use in a list format
    :return: the request payload
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
    Converts the name into the correct format to be inserted into the metadata field

    :metadata: cell/column metadata
    :uriReconciliator: the URI of the affiliated knowledge graph
    :return: metadata in the correct format
    """
    for item in metadata:
        item['entity'] = parseNameEntities(item['entity'], uriReconciliator)
    return metadata


def parseNameEntities(entities, uriReconciliator):
    """
    Function iterated in parseNameMetadata, works at the entity level

    :entities: entities present in the cell/column
    :uriReconciliator: the URI of the affiliated knowledge graph
    :return: entities in the correct format
    """
    for entity in entities:
        entity['name'] = parseNameField(
            entity['name'], uriReconciliator, entity['id'].split(':')[1])
    return entities


def parseNameField(name, uriReconciliator, idEntity):
    """
    The actual function that changes the name format to the one required for visualization

    :name: entity name
    :uriReconciliator: the URI of the affiliated knowledge graph
    :idEntity: entity ID
    :return: the name in the correct format
    """
    return {
        'value': name,
        'uri': uriReconciliator + idEntity
    }


def parseTable(table):
    """
    Obtains the table in parsed format, as a dataframe

    :table: table in raw format
    :return: a dataframe representing the table in parsed format
    """
    table = json.loads(table)
    columnNames = ["tableIndex"]
    columnNames.extend(list(table["columns"].keys()))
    dfTable = pd.DataFrame(columns=columnNames)
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
