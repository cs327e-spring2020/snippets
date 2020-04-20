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
    

def run():
    options = {
        'project': 'cs327e-sp2020', # change to your project id
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

        # write to files
        actor_pcoll | 'Write File' >> WriteToText('actor_output.txt')
        actress_pcoll | 'Write File' >> WriteToText('actress_output.txt')

    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()