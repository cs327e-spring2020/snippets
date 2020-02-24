import apache_beam as beam
from apache_beam.io import WriteToText
import logging

class SplitIntoWordsDoFn(beam.DoFn):
  def process(self, element):
     words = element.split()
     return [words]
           
def run():
     PROJECT_ID = 'cs327e-sp2020' # change to your project id

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     in_pcoll = p | beam.Create(['Hello Beam', 'This is awesome!'])

     out_pcoll = in_pcoll | 'Multiply' >> beam.ParDo(SplitIntoWordsDoFn())
        
     out_pcoll | 'Write results' >> WriteToText('split_words.txt')
    
     result = p.run()
     result.wait_until_finish() 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

    
 