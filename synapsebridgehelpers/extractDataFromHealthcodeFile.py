# libraries
import pandas as pd
import synapseclient
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
syn = synapseclient.login()

# tables and files of interest
PROJECTID = '' # synapse project to upload data to (e.g. 'syn1901847' for Wondrous Research Example)
TABLENAME = '' # name to give to table being uploaded to Synapse (e.g. 'My Data Table')
FILEID = '' # synapse file where healthcodes are stored (e.g. synId of csv file with healthcodes)
INPUT_TABLE_SYNID = '' # table to pull data from. This will eventually be a input from step one - running exploreCompliance.py. (e.g. synId of Lily Memory Activity Table - v1)


# grab healthcodes from Synapse
sampleIds = syn.get(FILEID)
samplePath = sampleIds.path

# read in the healthcodes of interest from running exploreCompliance.ipynb
healthCode = pd.read_csv(samplePath)

#use header=None if no headers are present in file and add in column names
# healthCode = pd.read_csv(samplePath, header=None) 
# healthCode.columns = ['healthCode', 'roc_id']

# grab list of healthCodes
healthCodeList = healthCode['healthCode'].tolist()

# Query table of interest
actv_syntable = syn.tableQuery("SELECT * FROM %s" %INPUT_TABLE_SYNID+ " WHERE healthCode IN ('"+"','".join(healthCodeList)+"')")

# convert to dataframe
df = actv_syntable.asDataFrame()

# TODO: convert filehandles to string

# upload to Synapse
columns = as_table_columns(df)
schema = Schema(name=TABLENAME, columns=columns, parent=PROJECTID)
table = syn.store(Table(schema, df))


