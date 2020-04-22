import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatTimestampFn(beam.DoFn):
  def process(self, element):
    event_record = element
    location_id = event_record.get('location_id')
    last_update = event_record.get('last_update')
    #print('last_update: ' + last_update)

    # reformat any timestamps which are MM/DD/YYYY HH:MM to YYYY-MM-DDTHH:MM:DD:SS
    # Example: 2020-01-31T10:37:00
    split_date = last_update.split('/')
    if len(split_date) > 1:
        
        month = split_date[0]
        day = split_date[1]
        year = split_date[2].split(' ')[0]
        
        if len(year) == 2:  # year needs to be YYYY
            year = '20' + year;
            
        if len(month) == 1:  # month needs to be MM
            month = '0' + month
            
        if len(day) == 1:  # day needs to be DD
            day = '0' + day
        
        date = year + '-' + month + '-' + day
        
        hour_min = last_update.split(' ')[1]
        hour = hour_min.split(':')[0]
        minute = hour_min.split(':')[1]
        
        if len(hour) == 1:  # hour needs to be HH
            hour = '0' + hour
        
        if len(minute) == 1:  # minute needs to be MM
            minute = '0' + minute
        
        time = hour + ":" + minute + ":00"
        last_update = date + "T" + time
        
        #print('new last_update: ' + last_update)
        
        event_record['last_update'] = last_update
    
    # return tuple for GroupByKey
    key = str(location_id) + ';' + last_update
    event_tuple = (key, event_record)
    return [event_tuple]

class RemoveDuplicatesFn(beam.DoFn):
    def process(self, element):
        key, event_obj = element # event_obj is an _UnwindowedValues object
        #print("key = " + key)
        
        location_id, last_update = key.split(';')
        
        event_list = list(event_obj) # cast to list type
        
        max_confirmed = None
        highest_confirmed_index = 0 # default to 0
        
        for i in range(len(event_list)):
            confirmed = event_list[i].get('confirmed', 0)
            
            #print('confirmed: ' + str(confirmed))
            #print('index: ' + str(i))
            
            if confirmed != None and max_confirmed != None:
                if confirmed > max_confirmed:
                    highest_confirmed_index = i
                    max_confirmed = confirmed
            
            elif confirmed != None and max_confirmed == None:
                highest_confirmed_index = i
                max_confirmed = confirmed
                
            elif confirmed == None and max_confirmed != None:
                highest_confirmed_index = highest_confirmed_index # don't do anything
            
            elif confirmed == None and max_confirmed == None:
                highest_confirmed_index = i
        
        #print('highest_confirmed_index: ' + str(highest_confirmed_index))
        
        return [event_list[highest_confirmed_index]]


def run():
    PROJECT_ID = 'cs327e-sp2020' # change to your project id
    BUCKET = 'gs://beam-output-data' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'event'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    p = Pipeline(options=options)

    sql = 'SELECT * FROM covid_19_modeled.Event_SQL_1'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # format timestamp   
    ts_pcoll = query_results | 'Format Timestamp' >> beam.ParDo(FormatTimestampFn())
         
    # group by primary key
    grouped_pcoll = ts_pcoll | 'Group by PK' >> beam.GroupByKey()
         
    # remove duplicate records
    unique_pcoll = grouped_pcoll | 'Remove Duplicates' >> beam.ParDo(RemoveDuplicatesFn())

    # write new PCollection to log file
    unique_pcoll | 'Write log' >> WriteToText(DIR_PATH + 'unique_pcoll.txt')
        
    dataset_id = 'covid_19_modeled'
    table_id = 'Event_Beam_DF'
    schema_id = '''location_id:INTEGER,last_update:DATETIME,confirmed:INTEGER,deaths:INTEGER,recovered:INTEGER,active:INTEGER'''

    # write PCollection to BQ table
    unique_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()