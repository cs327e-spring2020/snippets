import logging
import apache_beam as beam
from apache_beam.io import WriteToText

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
     PROJECT_ID = 'myproject' # change to your project id

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM covid_19_modeled.Location_Temp'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     # extract city from state 
     state_pcoll = query_results | 'Format State' >> beam.ParDo(FormatStateFn())
        
     grouped_pcoll = state_pcoll | 'Group Locations' >> beam.GroupByKey()
    
     unique_pcoll = grouped_pcoll | 'Remove Duplicates' >> beam.ParDo(RemoveDuplicatesFn())

     dataset_id = 'covid_19_modeled'
     table_id = 'Location_Beam2'
     schema_id = 'id:INTEGER,city:STRING,state:STRING,country:STRING,latitude:NUMERIC,longitude:NUMERIC'

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