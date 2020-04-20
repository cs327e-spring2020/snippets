import logging
import apache_beam as beam
from apache_beam.io import WriteToText

class FormatTimestampFn(beam.DoFn):
  def process(self, element):
    event_record = element
    last_update = event_record.get('last_update')
    #print('last_update: ' + last_update)

    # reformat any timestamps which are MM/DD/YYYY HH:MM to YYYY-MM-DD HH:MM:DD
    split_date = last_update.split('/')
    if len(split_date) > 1:
        
        month = split_date[0]
        day = split_date[1]
        year = split_date[2].split(' ')[0]
        
        if len(year) == 2:  # year needs to be YYYY
            year = '20' + year;
        
        date = year + '-' + month + '-' + day
        
        hour_min = last_update.split(' ')[1]
        time = hour_min + ":00"
        last_update = date + " " + time
        
        #print('new last_update: ' + last_update)
        
        event_record['last_update'] = last_update
    
    return [event_record]
    
def run():
     PROJECT_ID = 'myproject' # change to your project id

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM covid_19_modeled.Event_Temp'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

     # format timestamp   
     ts_pcoll = query_results | 'Format Timestamp' >> beam.ParDo(FormatTimestampFn())

     # write new PCollection to log file
     ts_pcoll | 'Write log' >> WriteToText('ts_pcoll.txt')
        
     dataset_id = 'covid_19_modeled'
     table_id = 'Event_Beam1'
     schema_id = '''location_id:INTEGER,last_update:DATETIME,
                    confirmed:INTEGER,deaths:INTEGER,recovered:INTEGER'''

     # write new PCollection to BQ table
     ts_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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