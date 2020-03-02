import logging
import apache_beam as beam
from apache_beam.io import WriteToText

class NormalizeDoFn(beam.DoFn):
  
    def process(self, element, class_pcoll):
        takes_record = element
        sid = takes_record.get('sid')
        cno = takes_record.get('cno')
        grade = takes_record.get('grade')
        
        cno_splits = cno.split('-')  # if we have a bad record, it will have a '-' (e.g. 'CS326E - Fall18')
        valid_cno = cno_splits[0].strip()
        
        for class_record in class_pcoll:
            class_cid = class_record.get('cid')
            class_cno = class_record.get('cno')
            
            if valid_cno == class_cno:
                takes_record['cid'] = class_cid  # assumes we want the first match
                break
                
        del takes_record['cno']
    
        return [takes_record]
            
         
def run():
    PROJECT_ID = 'cs327e-sp2020' # change to your project id

    # Project ID is required when using the BQ source
    options = {
      'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DirectRunner', options=opts)

    takes_sql = 'SELECT sid, cno, grade FROM college_modeled.Takes'
    class_sql = 'SELECT cid, cno FROM college_modeled.Class'

    takes_pcoll = p | 'Read from BQ Takes' >> beam.io.Read(beam.io.BigQuerySource(query=takes_sql, use_standard_sql=True))
    class_pcoll = p | 'Read from BQ Class' >> beam.io.Read(beam.io.BigQuerySource(query=class_sql, use_standard_sql=True))

    # write PCollections to log files
    #takes_pcoll | 'Write log 1' >> WriteToText('takes_query_results.txt')
    #class_pcoll | 'Write log 2' >> WriteToText('class_query_results.txt')

    # ParDo with side-input 
    norm_takes_pcoll = takes_pcoll | 'Normalize Record' >> beam.ParDo(NormalizeDoFn(), beam.pvalue.AsList(class_pcoll))

    # write PCollection to log file
    norm_takes_pcoll | 'Write log 3' >> WriteToText('norm_takes_pcoll.txt')

    dataset_id = 'college_modeled'
    table_id = 'Takes_Beam'
    schema_id = 'sid:STRING,cid:STRING,grade:STRING'

    # write PCollection to new BQ table
    norm_takes_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
