#! /usr/bin/env python
#
# LittleBigQuery, a sane python wrapper for BigQuery
#
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient import discovery
from googleapiclient.http import MediaFileUpload
from oauth2client.client import GoogleCredentials
import uuid, time
from datetime import datetime
import json
import pandas as pd

class LittleBigQueryException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

class LittleBigQuery(object):
    """
    The LittleBigQuery class.  This is the primary class for the wrapper.
    
    Attributes:
        projectId: the current projectId
        credentials: the GoogleCredentials for this session
        bigquery_service: the authenticated BQ v2 API tokens
    """
    
    def __init__(self, projectId, dataset=None):
        self.credentials = GoogleCredentials.get_application_default()
        self.bigquery_service = build('bigquery', 'v2', credentials=self.credentials)
        self.project_id = projectId
        self.dataset = dataset
        
    def _poll_job(self, job):
        """Waits for a job to complete.  Adapted from the 
        Google BigQuery Samples
        """

        print('Waiting for job to finish...')

        request = self.bigquery_service.jobs().get(
            projectId=self.project_id,
            jobId=job['jobReference']['jobId'])

        while True:
            result = request.execute(num_retries=2)

            if result['status']['state'] == 'DONE':
                if 'errorResult' in result['status']:
                    raise RuntimeError(result['status']['errorResult'])
                print('Job complete.')
                return True

            time.sleep(1)
        return False
    
    def _parse_schema(self, s):
        names_and_types = map(lambda x: (x["name"], x["type"]), 
            s["fields"])
        def set_type(t):
            return {
                "INTEGER":int,
                "FLOAT":float,
                "STRING":str,
                "BOOLEAN": lambda x : x.upper()=="TRUE" and True or False,
                "TIMESTAMP": lambda x: datetime.fromtimestamp(float(x)),
                "RECORD": lambda x: json.dumps(x)
            }.get(t)
        mappings = []
        for n in names_and_types:
            mappings.append((n[0], set_type(n[1])))

        return mappings
        
    def _apply_schema(self, r, s):
        raw_data = map(lambda x: map(lambda y: y["v"], x["f"]), r)

        def _encode(this_row, sch):
            new_row = []
            for i in range(len(sch)):

                new_row.append(sch[i][1](this_row[i]))
            return new_row
        
        return map(lambda x: _encode(x, s), raw_data)
        
    def query(self, q, raw=False, sync=False, projectId=None):
        """
        Default query method.  Takes a query and submits it to
        the BigQuery web service.  By default, uses the 
        configured projectId, performs an async query, and 
        returns the result as a pandas data frame.
        
        Arguments:
            q: the query
            raw: Returns the raw result
            sync: Async (default) or sync operation
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments", "little_big_query_test")
        >>> BQ.query("SELECT COUNT(*) as trip_count FROM [nyc-tlc:yellow.trips];")
        Waiting for job to finish...
        Job complete.
           trip_count
        0  1108779463
        """
        # get a new job ID
        job_id = str(uuid.uuid4())
        
        # structure the request
        request = {
            "jobReference" : {
                "projectId" : self.project_id,
                "job_id" : job_id
            },
            "configuration" : {
                "query" :{
                    "query" : q,
                    "defaultDataset" : {
                        "projectId" : self.project_id,
                        "datasetId" : self.dataset
                    },
                    "priority" : 'INTERACTIVE'                    }
            }
        }

        this_job = self.bigquery_service.jobs().insert(
            projectId=self.project_id,
            body=request).execute(num_retries=5)

        ready = self._poll_job(this_job)
        if ready:
            raw_results = self.bigquery_service.jobs().getQueryResults(
                projectId=this_job['jobReference']['projectId'], 
                jobId=this_job['jobReference']['jobId']).execute()
            if raw:
                return raw_results
            rows = raw_results["rows"]
            schema = self._parse_schema(raw_results["schema"])
            frame = pd.DataFrame(self._apply_schema(rows, schema))
            frame.columns = map(lambda x: x[0], schema)
            return frame
            
    ### essential DBMS functions
    def createTable(self, tableName, datasetId=None):
        if not datasetId:
                datasetId = self.dataset
        #create an empty table
        table_service = self.bigquery_service.tables()
        table_service.insert(projectId=self.project_id,
            datasetId=datasetId, body={
            "tableReference" :{
                "projectId":self.project_id,
                "tableId":tableName,
                "datasetId":datasetId
            }
        }).execute()
        
        
    #createPartitionedTable
    def createPartitionedTable(self, tableName, expiration=0, datasetId=None):
        if not datasetId:
                datasetId = self.dataset
        #create an empty table
        partitioning = {"type": "DAY"}
        if expiration > 0:
            partitioning["expirationMs"] = str(expiration)
            
        table_service = self.bigquery_service.tables()
        table_service.insert(projectId=self.project_id,
            datasetId=datasetId, body={
            "tableReference" :{
                "projectId":self.project_id,
                "tableId":tableName,
                "datasetId":datasetId
            },
            "timePartitioning": partitioning
        }).execute()
        
    #dropTable
    def dropTable(self, tableId, datasetId=None):
        if not datasetId:
            datasetId = self.dataset
            
        self.bigquery_service.tables().delete(projectId=self.project_id, 
            datasetId=datasetId, tableId=tableId).execute()
    
    #partitionTable
    #listPartitions
    def showPartitions(self, tableName, datasetId=None):
        if not datasetId:
                datasetId = self.dataset
        q = "select partition_id from [%s.%s.__PARTITIONS_SUMMARY]__" % (tableName, datasetId)
        return self.query(q)
    
    #describeTable
    def desc(self, tableName, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromCSV("my_gcs_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "gs://little_big_query_test/csv/*", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.desc("my_gcs_table")
        [{u'type': u'INTEGER', u'name': u'id'}, {u'type': u'STRING', u'name': u'email'}, {u'type': u'FLOAT', u'name': u'amount'}, {u'type': u'TIMESTAMP', u'name': u'event_time'}]
        >>> BQ.dropTable("my_gcs_table")
        """
        if not datasetId:
                datasetId = self.dataset
        
        t = self.bigquery_service.tables()
        ts = t.get(projectId=self.project_id, datasetId=datasetId, tableId=tableName).execute()
        return ts["schema"]["fields"]
        
    #createTableAsSelect
    def createTableAsSelect(self, q, tableName, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromCSV("my_gcs_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "gs://little_big_query_test/csv/*", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.createTableAsSelect("select sum(amount) from [little_big_query_test.my_gcs_table]", "my_agg_table")
        Waiting for job to finish...
        Job complete.
        >>> BQ.query("select count(*) from my_agg_table")
        Waiting for job to finish...
        Job complete.
           f0_
        0    1
        >>> BQ.dropTable("my_gcs_table")
        >>> BQ.dropTable("my_agg_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
        
        #create an empty table
        table_service = self.bigquery_service.tables()
        table_service.insert(projectId=self.project_id,
            datasetId=datasetId, body={
            "tableReference" :{
                "projectId":self.project_id,
                "tableId":tableName,
                "datasetId":datasetId
            }
        }).execute()
        # get a new job ID
        job_id = str(uuid.uuid4())
        
        #query and fill the table
        request = {
            "jobReference" : {
                "projectId" : self.project_id,
                "job_id" : job_id
            },
            "configuration" : {
                "query" :{
                    "query" : q,
                    "priority" : 'INTERACTIVE',
                    "destinationTable" : {
                        "projectId" : self.project_id,
                        "datasetId": datasetId,
                        "tableId" : tableName
                        },
                    "allowLargeResults" : 'true'
                    }
            }
        }
        this_job = self.bigquery_service.jobs().insert(
            projectId=self.project_id,
            body=request).execute(num_retries=5)
        ready = self._poll_job(this_job)
            
    #useDataset
    def useDataset(self, datasetId):
        self.dataset = datasetId
        
    #createDataset
    def createDataset(self, datasetName, description=None):
        """
        Create a new dataset.
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.createDataset("bigger_query_test", "Test from Bigger Query")
        True
        >>> BQ.deleteDataset("bigger_query_test")
        """
        add_description = False
        if description:
            add_description = True
            
        request = {
            "description" : description if add_description else "Dataset created by bigger query",
            "datasetReference" : {
                "projectId" : self.project_id,
                "datasetId" : datasetName
                }
            }
        ds = self.bigquery_service.datasets()
        ds.insert(projectId=self.project_id, body=request).execute()
        #check to make sure it's been created
        try:
            ds.get(projectId=self.project_id, datasetId=datasetName)
            return True
        except:
            return False
        
    #deleteDataset
    def deleteDataset(self, datasetName, deleteContents=False):
        """
        Delete a dataset.
        """
        ds = self.bigquery_service.datasets()
        try:
            ds.delete(projectId=self.project_id, 
                datasetId=datasetName, deleteContents=deleteContents).execute()
        except:
            pass

    #showDatasets
    def showDatasets(self):
        """
        List all available datasets

        >>> BQ = LittleBigQuery("nyc-tlc", "yellow")
        >>> BQ.showDatasets()
        [u'green', u'yellow']
        """
        ds = self.bigquery_service.datasets()
        return map(lambda x: x["datasetReference"]["datasetId"], 
            ds.list(projectId=self.project_id).execute()["datasets"])
            
    def datasets(self):
        """
        Alias for showDatasets
        """
        return self.showDatasets()
            
    #showTables
    def showTables(self, datasetId=None):
        """
        List all tables in a dataset.  If no datasetId is provided,
        attempt to use the default datasetId specified in the class.
        If neither is present, raise an exception.
        >>> BQ = LittleBigQuery("nyc-tlc", "green")
        >>> BQ.showTables()
        [u'trips_2014', u'trips_2015']
        """
        
        ts = self.bigquery_service.tables()
        dsID = None
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                dsID = self.dataset
        else:
            dsID = datasetId
        listing = ts.list(projectId=self.project_id, 
            datasetId=dsID).execute()
        return map(lambda x: x["tableReference"]["tableId"], 
            listing["tables"])
            
    def tables(self, datasetId=None):
        """
        Alias for showTables
        """
        return self.showTables(datasetId)
        
    #listProjects
    def listProjects(self):
        """
        List all projects this user has access to.
        """
        
        prj = self.bigquery_service.projects().list().execute()
        return map(lambda x: {"friendlyName":x["friendlyName"], 
            "projectId":x["projectReference"]["projectId"]}, 
            prj["projects"])
            
    def projects(self):
        """
        Alias for listProjects
        """
        return self.listProjects()
            
    #createTableFromFrame
    
    #createTableFromSheet
    def createTableFromSheet(self, tableName, schema, sheet_url, skip_rows=0, datasetId=None):
        """
        Create a BigQuery Table from a Google Sheet stored in Google Drive.
        sheet_url must be a link to Google Drive.
        
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromSheet("my_sheet_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "https://docs.google.com/spreadsheets/d/1rG30jAhUXY5vVKYIe15CwveytIeq2k_kAzyoWMt-75I/edit?usp=sharing", "little_big_query_test")
        >>> BQ.dropTable("my_sheet_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
        
        # structure the request
        request = {
                  "externalDataConfiguration": {
                   "sourceUris": [
                    sheet_url
                   ],
                   "schema": {
                    "fields": [{"name": i[0], "type":i[1]} for i in schema]},
                   "sourceFormat": "GOOGLE_SHEETS",
                   "googleSheetsOptions": {
                    "skipLeadingRows": "0"
                   }
                  },
                  "tableReference": {
                   "projectId": self.project_id,
                   "datasetId": datasetId,
                   "tableId": tableName
                  }
                 }
        
        this_job = self.bigquery_service.tables().insert(
            projectId=self.project_id, datasetId=datasetId,
            body=request).execute()
        
    #createTableFromCSV
    def appendTableFromCSV(self, tableName, schema, gcs_path, datasetId=None):
        self.createTableFromCSV(tableName, schema, gcs_path, datasetId)
        
    def createTableFromCSV(self, tableName, schema, gcs_path, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromCSV("my_gcs_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "gs://little_big_query_test/csv/*", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.query("select count(*) from [little_big_query_test.my_gcs_table]")
        Waiting for job to finish...
        Job complete.
           f0_
        0   10
        >>> BQ.dropTable("my_gcs_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
                
        job_id = str(uuid.uuid4())
        
        # structure the request
        request = {
            "jobReference" : {
                "projectId" : self.project_id,
                "job_id" : job_id
                },
            'configuration' : {
                'load' : {
                    'sourceUris' : [gcs_path],
                    'schema' : {
                        'fields' : [{"name":i[0], "type":i[-1]} for i in schema]
                    },
                    'destinationTable' : {
                        'projectId' : self.project_id,
                        'datasetId' : datasetId,
                        'tableId' : tableName
                    },
                    'source_format' : "CSV"
                }
            } 
        }
        insert_job = self.bigquery_service.jobs().insert(projectId=self.project_id,
            body=request)
        
        this_job = insert_job.execute(num_retries=5)
        self._poll_job(this_job)
    
    def appendTableFromJSON(self, tableName, schema, gcs_path, datasetId=None):
        self.createTableFromJSON(tableName, schema, gcs_path, datasetId)
            
    def createTableFromJSON(self, tableName, schema, gcs_path, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromJSON("my_json_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "gs://little_big_query_test/json/*", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.query("select count(*) from [little_big_query_test.my_json_table]")
        Waiting for job to finish...
        Job complete.
           f0_
        0   20
        >>> BQ.dropTable("my_json_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
            
        job_id = str(uuid.uuid4())
        
        # structure the request
        request = {
         "jobReference": {
          "projectId": self.project_id,
          "jobId": job_id
         },
         "configuration": {
          "load": {
           'destinationTable' : {
               'projectId' : self.project_id,
               'datasetId' : datasetId,
               'tableId' : tableName
           },
           "sourceFormat": "NEWLINE_DELIMITED_JSON",
           "sourceUris": [
            gcs_path
           ],
           "schema": {
            "fields": [{"name":f[0], "type": f[1]} for f in schema]
           }
          }
         }
        }

        insert_job = self.bigquery_service.jobs().insert(projectId=self.project_id,
            body=request)
        
        this_job = insert_job.execute(num_retries=5)
        self._poll_job(this_job)

    def appendTableFromAvro(self, tableName, schema, gcs_path, datasetId=None):
        self.createTableFromAvro(tableName, schema, gcs_path, datasetId)
            
    def createTableFromAvro(self, tableName, schema, gcs_path, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromAvro("my_avro_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "gs://little_big_query_test/avro/*", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.query("select count(*) from [little_big_query_test.my_avro_table]")
        Waiting for job to finish...
        Job complete.
           f0_
        0   10
        >>> BQ.dropTable("my_avro_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
        job_id = str(uuid.uuid4())
        
        # structure the request
        request = {
         "jobReference": {
          "projectId": self.project_id,
          "jobId": job_id
         },
         "configuration": {
          "load": {
           "destinationTable": {
            "projectId": self.project_id,
            "datasetId": datasetId,
            "tableId": tableName
           },
           "sourceFormat": "AVRO",
           "sourceUris": [
            gcs_path
           ],
          "schema": {
           "fields": [{"name":f[0], "type": f[1]} for f in schema]
           }
          }
         }
        }
        
        insert_job = self.bigquery_service.jobs().insert(projectId=self.project_id,
            body=request)
        
        this_job = insert_job.execute(num_retries=5)
        self._poll_job(this_job)
        
    #createTableFromLocalCSV
    def appendTableFromLocalCSV(self, tableName, schema, data_path, datasetId=None):
        self.createTableFromLocalCSV(tableName, schema, data_path, datasetId)
        
    def createTableFromLocalCSV(self, tableName, schema, data_path, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromLocalCSV("my_csv_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "examples/MOCK_DATA.csv", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.query("select count(*) from [little_big_query_test.my_csv_table]")
        Waiting for job to finish...
        Job complete.
           f0_
        0   10
        >>> BQ.dropTable("my_csv_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
            
        request = {
            'configuration' : {
                'load' : {
                    'schema' : {
                        'fields' : [{"name":i[0], "type":i[-1]} for i in schema]
                    },
                    'destinationTable' : {
                        'projectId' : self.project_id,
                        'datasetId' : datasetId,
                        'tableId' : tableName
                    },
                    'source_format' : "CSV"
                }
            } 
        }
        mediaBody = MediaFileUpload(data_path,mimetype='application/octet-stream')
        insert_job = self.bigquery_service.jobs().insert(projectId=self.project_id,
            body=request, media_body=mediaBody)
        
        this_job = insert_job.execute()
        self._poll_job(this_job)
        
    #createExternalTable
    #TODO later
    
    #grantAccessByEmail
    #grantAccessByDomain
    #createView
    def createView(self, q, viewName, datasetId=None):
        """
        >>> BQ = LittleBigQuery("google.com:pd-pm-experiments")
        >>> BQ.useDataset("little_big_query_test")
        >>> BQ.createTableFromJSON("my_json_table", [("id", "INTEGER"), ("email", "STRING"), ("amount", "FLOAT"), ("event_time", "TIMESTAMP")], "gs://little_big_query_test/json/*", "little_big_query_test")
        Waiting for job to finish...
        Job complete.
        >>> BQ.createView("SELECT amount FROM [little_big_query_test.my_json_table]", "my_new_view", "little_big_query_test")
        >>> BQ.showTables()
        [u'my_json_table', u'my_new_view']
        >>> BQ.dropTable("my_new_view")
        >>> BQ.dropTable("my_json_table")
        """
        if not datasetId:
            if not self.dataset:
                raise LittleBigQueryException("No datasetId specified.")
            else:
                datasetId = self.dataset
        
        if "[" not in q and "]" not in q:
            #lame way to check for dataset reference in query
            raise LittleBigQueryException("View creation queries must reference [dataset.table]")
            
        request = {
         "view": {
          "query": q
         },
         "tableReference": {
          "projectId": self.project_id,
          "datasetId": datasetId,
          "tableId": viewName
         }
        }
        
        table_job = self.bigquery_service.tables().insert(projectId=self.project_id,
            datasetId=datasetId, body=request).execute()
        
        
    
    
    

if __name__ == "__main__":
    import doctest
    doctest.testmod()