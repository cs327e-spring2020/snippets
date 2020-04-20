import logging
import apache_beam as beam
from apache_beam.io import WriteToText

class FormatStateFn(beam.DoFn):
  def process(self, element):
    loc_record = element
    state = loc_record.get('state')
    
    if state == None:
        return [loc_record]
    
    #print('state: ' + state)

    split_state = state.split(',')
    if len(split_state) > 1:
        city = split_state[0]
        state = split_state[1]
        
        loc_record['city'] = city
        loc_record['state'] = state
        
        #print('city: ' + city)
        #print('state: ' + state)
    
    return [loc_record]
    
    
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
        
     # format state 
     state_pcoll = query_results | 'Format State' >> beam.ParDo(FormatStateFn())

     # write new PCollection to log file
     state_pcoll | 'Write log' >> WriteToText('state_pcoll.txt')

     dataset_id = 'covid_19_modeled'
     table_id = 'Location_Beam1'
     schema_id = 'id:INTEGER,city:STRING,state:STRING,country:STRING,latitude:NUMERIC,longitude:NUMERIC'

     # write new PCollection to BQ table
     state_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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