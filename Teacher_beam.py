import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class StandardizeDoFn(beam.DoFn):
  def process(self, element):
    teacher_record = element
    tid = teacher_record.get('tid')
    instructor = teacher_record.get('instructor')
    dept = teacher_record.get('dept')

    # parse first and last names and store them as separate fields
    split_name = instructor.split(',')
    if len(split_name) > 1:
        last_name = split_name[0]
        first_name = split_name[1]
    else:
        split_name = instructor.split(' ')
        first_name = split_name[0]
        last_name = split_name[1]
    
    teacher_record.pop('instructor')
    formatted_first_name = first_name.title().strip()
    formatted_last_name = last_name.title().strip()
    teacher_record['fname'] = formatted_first_name
    teacher_record['lname'] = formatted_last_name
    
    # rename department if it's abbreviated
    if dept == 'CS':
        teacher_record['dept'] = 'Computer Science'
    if dept == 'Math':
        teacher_record['dept'] = 'Mathematics'
    
    # create key, value pairs
    teacher_tuple = (tid, teacher_record)
    return [teacher_tuple]

class DedupRecordsDoFn(beam.DoFn):
  def process(self, element):
     tid, teacher_obj = element # teacher_obj is an _UnwindowedValues type
     teacher_list = list(teacher_obj) # cast to list 
     teacher_record = teacher_list[0] # grab the first record
     print('teacher_record: ' + str(teacher_record))
     return [teacher_record]  

def run():
    PROJECT_ID = 'cs327e-sp2020' # change to your project id

    # Project ID is required when using the BQ source
    options = {
     'project': PROJECT_ID
    }
        
    opts = beam.pipeline.PipelineOptions(flags=[], **options)
    
    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT tid, instructor, dept FROM college_modeled.Teacher limit 50'
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=sql, use_standard_sql=True))

    query_results | 'Write log 1' >> WriteToText('query_results.txt')
  
    teacher_pcoll = query_results | 'Standardize' >> beam.ParDo(StandardizeDoFn())

    teacher_pcoll | 'Write log 2' >> WriteToText('formatted_teacher_pcoll.txt')

    # group records by tid
    grouped_pcoll = teacher_pcoll | 'Group by tid' >> beam.GroupByKey()

    grouped_pcoll | 'Write log 3' >> WriteToText('grouped_teacher.txt')

    # remove duplicates
    distinct_pcoll = grouped_pcoll | 'Dedup' >> beam.ParDo(DedupRecordsDoFn())

    distinct_pcoll | 'Write log 4' >> WriteToText('distinct_teacher.txt')

    dataset_id = 'college_modeled'
    table_id = 'Teacher_Beam'
    schema_id = 'tid:STRING,fname:STRING,lname:STRING,dept:STRING'

    distinct_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                            table=table_id, 
                                            schema=schema_id,
                                            project=PROJECT_ID,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                            batch_size=int(1000))
                                            
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()