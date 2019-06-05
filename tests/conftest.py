import os
import pytest
import pandas
import tempfile
import synapseclient
import uuid

SAMPLE_TABLE = "tests/sample_table.csv"

@pytest.fixture(scope='session')
def syn():
    syn = synapseclient.login()
    return syn


def read(obj):
    if isinstance(obj, str):
        return pandas.read_csv(obj, header=0, index_col=None)
    else:
        return obj


@pytest.fixture(scope='session')
def sample_table(syn, project):
    sample_table_no_file_handles = read(SAMPLE_TABLE)
    sample_files = [file_(syn, project) for i in
                    range(len(sample_table_no_file_handles))]
    file_handles = [int(f["dataFileHandleId"]) for f in sample_files]
    sample_table_no_file_handles["raw_data"] = file_handles
    return sample_table_no_file_handles


@pytest.fixture
def new_project(syn):
    project = synapseclient.Project(str(uuid.uuid4()))
    project = syn.store(project)
    yield project
    syn.delete(project)


@pytest.fixture(scope='session')
def project(syn):
    project = synapseclient.Project(str(uuid.uuid4()))
    project = syn.store(project)
    yield project
    syn.delete(project)


def file_(syn, parent):
    """Store a randomly generated file to Synapse"""
    f = tempfile.NamedTemporaryFile(suffix=".csv")
    with open(f.name, 'wb') as fout:
        fout.write(os.urandom(2))
    file_ = synapseclient.File(path=f.name, parent=parent)
    file_ = syn.store(file_)
    return file_

def table_schema(project_obj):
    cols = [synapseclient.Column(name="recordId", columnType="INTEGER"),
            synapseclient.Column(name="externalId", columnType="STRING"),
            synapseclient.Column(name="substudyMemberships", columnType="STRING"),
            synapseclient.Column(name="bool_property", columnType="BOOLEAN"),
            synapseclient.Column(name="str_property", columnType="STRING"),
            synapseclient.Column(name="raw_data", columnType="FILEHANDLEID")]
    schema = synapseclient.Schema(name = str(uuid.uuid4()),
                                  columns = cols,
                                  parent = project_obj["id"])
    return schema


def table(syn, parent, sample_table, schema=None):
    if schema is None:
        t = synapseclient.table.build_table(
                name=str(uuid.uuid4()),
                parent=parent,
                values=sample_table)
    else:
        t = synapseclient.Table(schema=schema, values=sample_table)
    table = syn.store(t)
    return table.schema


@pytest.fixture(scope='session')
def tables(syn, project, sample_table):
    # store a sample table
    schemas = [table(syn, project, sample_table, table_schema(project))
               for i in range(2)]
    columns = [list(syn.getColumns(t["id"])) for t in schemas]
    tables = {"schema": schemas, "columns": columns}
    return tables

@pytest.fixture
def new_tables(syn, sample_table, new_project):
    # store a sample table
    schemas = [table(syn, new_project, sample_table, table_schema(new_project))
               for i in range(2)]
    schemas = [syn.get(s["id"]) for s in schemas]
    columns = [list(syn.getColumns(t["id"])) for t in schemas]
    tables = {"schema": schemas, "columns": columns}
    return tables
