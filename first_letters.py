import apache_beam as beam
from apache_beam.io import WriteToText
import logging

class ExtractFirstLetterDoFn(beam.DoFn):
  def process(self, element):
     tuple = (element[0], element)
     return [tuple]
           
def run():
     PROJECT_ID = 'cs327e-sp2020' # change to your project id

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     in_pcoll = p | beam.Create(['apple', 'ball', 'car', 'bear', 'cheetah', 'ant'])

     out_pcoll = in_pcoll | 'Extract' >> beam.ParDo(ExtractFirstLetterDoFn())
    
     grouped_pcoll = out_pcoll | 'Grouped' >> beam.GroupByKey()
        
     grouped_pcoll | 'Write results' >> WriteToText('grouped_letters.txt')
    
     result = p.run()
     result.wait_until_finish() 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

    