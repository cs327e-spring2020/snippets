import os, logging
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn with multiple outputs
class ActorActressCountFn(beam.DoFn):
    
  OUTPUT_TAG_ACTOR_COUNT = 'tag_actor_count'
  OUTPUT_TAG_ACTRESS_COUNT = 'tag_actress_count'
  
  def process(self, element):
    year = element.get('year')
    category = element.get('category')
    winner = element.get('winner')
    entity = element.get('entity')
    actor_actress_name = entity.strip().title()

    if 'ACTOR' in category:
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_ACTOR_COUNT, (actor_actress_name, 1))  
        
    if 'ACTRESS' in category:
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_ACTRESS_COUNT, (actor_actress_name, 1))  

class ActorActressSumFn(beam.DoFn):
  def process(self, element):
     actor_actress_name, count_obj = element 
     count_list = list(count_obj) # cast to list to support len   
     total_count = len(count_list)
     return [(actor_actress_name, total_count)]  

class MakeBQRecordFn(beam.DoFn):
  def process(self, element):
     name, total_nominations = element
     record = {name, total_nominations}   
     return [record] 

def run():
    
    PROJECT_ID = 'cs327e-sp2020' # change to your project id
    
    options = {
        'project': PROJECT_ID, 
        'runner': 'DirectRunner',
        'streaming': False
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)
    
    with beam.Pipeline(options=opts) as p:

        query_string = 'SELECT * FROM Nomination_Events'
        query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query_string))

        #query_results | 'Write log' >> WriteToText('query_results.txt')
    
        out_pcoll = query_results | 'Extract Actor and Actress' >> beam.ParDo(ActorActressCountFn()).with_outputs(
                                                          ActorActressCountFn.OUTPUT_TAG_ACTOR_COUNT,
                                                          ActorActressCountFn.OUTPUT_TAG_ACTRESS_COUNT)
                                                          
        actor_pcoll = out_pcoll[ActorActressCountFn.OUTPUT_TAG_ACTOR_COUNT]
        actress_pcoll = out_pcoll[ActorActressCountFn.OUTPUT_TAG_ACTRESS_COUNT]
        
        grouped_actor_pcoll = actor_pcoll | 'Group by Actor' >> beam.GroupByKey()
        grouped_actress_pcoll = actress_pcoll | 'Group by Actress' >> beam.GroupByKey()
    
        #grouped_actor_pcoll | 'Write Actor log' >> WriteToText('grouped_actor_output.txt')
        #grouped_actress_pcoll | 'Write Actress log' >> WriteToText('grouped_actress_output.txt')

        summed_actor_pcoll = grouped_actor_pcoll | 'Sum up Actor Nominations' >> beam.ParDo(ActorActressSumFn())
        summed_actress_pcoll = grouped_actress_pcoll | 'Sum up Actress Nominations' >> beam.ParDo(ActorActressSumFn())

        # write to logs
        #summed_actor_pcoll | 'Write Actor File' >> WriteToText('summed_actor.txt')
        #summed_actress_pcoll | 'Write Actress File' >> WriteToText('summed_actress.txt')
        
        bq_actor_pcoll = summed_actor_pcoll | 'Make BQ Actor' >> beam.ParDo(MakeBQRecordFn())
        bq_actress_pcoll = summed_actress_pcoll | 'Make BQ Actress' >> beam.ParDo(MakeBQRecordFn())
        
        # write to logs
        #bq_actor_pcoll | 'Write Actor File 4' >> WriteToText('bq_actor.txt')
        #bq_actress_pcoll | 'Write Actress File 4' >> WriteToText('bq_actress.txt')
        
        dataset_id = 'oscars'
        actor_table_id = 'Actor_Nomination'
        actress_table_id = 'Actress_Nomination'
        table_schema = 'name:STRING,count:INTEGER'
        
        bq_actor_pcoll | 'Write Actor Table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=actor_table_id, 
                                                  schema=table_schema,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        
                                                    
        bq_actress_pcoll | 'Write Actress Table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=actress_table_id, 
                                                  schema=table_schema,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()