import os, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class ActorActressCountFn(beam.DoFn):
  def process(self, element):
    year = element.get('year')
    category = element.get('category')
    winner = element.get('winner')
    entity = element.get('entity')
    actor_actress_name = entity.strip().title()

    if 'ACTOR' in category or 'ACTRESS' in category:
        return [(actor_actress_name, 1)]    
    
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

        #query_results | 'Write log 1' >> WriteToText('query_results.txt')

        out_pcoll = query_results | 'Extract Actor' >> beam.ParDo(ActorActressCountFn())

        # write to ouput file
        out_pcoll | 'Write File' >> WriteToText('oscars_output.txt')

    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()