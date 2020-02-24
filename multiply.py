import apache_beam as beam
from apache_beam.io import WriteToText
import logging

class MultiplyDoFn(beam.DoFn):
  def process(self, element):
     return [element * 10]  
           
def run():
     PROJECT_ID = 'cs327e-sp2020' # change to your project id

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     in_pcoll = p | beam.Create([1, 2, 3, 4, 5])

     out_pcoll = in_pcoll | 'Multiply' >> beam.ParDo(MultiplyDoFn())
        
     out_pcoll | 'Write results' >> WriteToText('multiplied_numbers.txt')
    
     result = p.run()
     result.wait_until_finish() 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
    
    
   
