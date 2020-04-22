import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatStateFn(beam.DoFn):
  def process(self, element):
    loc_record = element
    
    id = loc_record.get('id')
    
    #print("id: " + str(id))
    
    tuple = (id, loc_record)
    
    state = loc_record.get('state')
    
    if state == None:
        return [tuple]
    
    #print('state: ' + state)

    split_state = state.split(',')
    if len(split_state) > 1:
        city = split_state[0]
        state = split_state[1]
        
        loc_record['city'] = city
        loc_record['state'] = state
        
        #print('city: ' + city)
        #print('state: ' + state)
        
    return [tuple]

class RemoveDuplicatesFn(beam.DoFn):
    def process(self, element):
        key, loc_obj = element # loc_obj is an _UnwindowedValues object
        loc_list = list(loc_obj) # cast to list type
        
        #print("id: " + str(key))
        
        max_lat = None
        highest_lat_index = 0 # default to 0
        
        for i in range(len(loc_list)):
            lat = loc_list[i].get('latitude', 0)
            
            #print("lat: " + str(lat))
            #print("lon: " + str(lon))
            
            if lat != None and max_lat != None:
                if lat > max_lat:
                    highest_lat_index = i
                    max_lat = lat
            
            elif lat != None and max_lat == None:
                highest_lat_index = i
                max_lat = lat
                
            elif lat == None and max_lat != None:
                highest_lat_index = highest_lat_index # don't do anything
            
            elif lat == None and max_lat == None:
                highest_lat_index = i
        
        #print('highest_confirmed_index: ' + str(highest_confirmed_index))
        
        return [loc_list[highest_lat_index]]
    
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

    sql = 'SELECT * FROM covid_19_modeled.Location_SQL_1'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
    # extract city from state 
    state_pcoll = query_results | 'Format State' >> beam.ParDo(FormatStateFn())
        
    grouped_pcoll = state_pcoll | 'Group Locations' >> beam.GroupByKey()
    
    unique_pcoll = grouped_pcoll | 'Remove Duplicates' >> beam.ParDo(RemoveDuplicatesFn())

    dataset_id = 'covid_19_modeled'
    table_id = 'Location_Beam_DF'
    schema_id = 'id:INTEGER,city:STRING,state:STRING,country:STRING,latitude:NUMERIC,longitude:NUMERIC,fips:INTEGER,admin:STRING,combined_key:STRING'

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