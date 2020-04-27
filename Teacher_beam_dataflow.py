import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

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
     teacher_record = teacher_list[0] # grab the first teacher record
     print('teacher_record: ' + str(teacher_record))
     return [teacher_record]  

def run():
    PROJECT_ID = 'cs327e-sp2020' # change to your project id
    BUCKET = 'gs://beam-output-data' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'teacher-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT tid, instructor, dept FROM college_workflow_modeled.Teacher'
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

    dataset_id = 'college_workflow_modeled'
    table_id = 'Teacher_Beam_DF'
    schema_id = 'tid:STRING,fname:STRING,lname:STRING,dept:STRING'

    distinct_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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