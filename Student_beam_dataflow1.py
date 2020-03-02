import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatDOBFn(beam.DoFn):
  def process(self, element):
    student_record = element
    sid = student_record.get('sid')
    fname = student_record.get('fname')
    lname = student_record.get('lname')
    dob = student_record.get('dob')
    print('current dob: ' + dob)

    # reformat any dob values that are MM/DD/YYYY to YYYY-MM-DD
    split_date = dob.split('/')
    if len(split_date) > 1:
        month = split_date[0]
        day = split_date[1]
        year = split_date[2]
        dob = year + '-' + month + '-' + day
        print('new dob: ' + dob)
        student_record['dob'] = dob
    
    # create key, value pairs
    student_tuple = (sid, student_record)
    return [student_tuple]

class DedupStudentRecordsFn(beam.DoFn):
  def process(self, element):
     sid, student_obj = element # student_obj is an _UnwindowedValues type
     student_list = list(student_obj) # cast to list to extract record
     student_record = student_list[0] # grab first student record
     print('student_record: ' + str(student_record))
     return [student_record]  
           
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
    google_cloud_options.job_name = 'student-df1'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
    
    sql = 'SELECT sid, fname, lname, dob FROM college_modeled.Student'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write PCollection to log file
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format the student's date of birth  
    formatted_dob_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatDOBFn())

    # write PCollection to log file
    formatted_dob_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'formatted_dob_pcoll.txt')

    # group students by sid
    grouped_student_pcoll = formatted_dob_pcoll | 'Group by sid' >> beam.GroupByKey()

    # write PCollection to log file
    grouped_student_pcoll | 'Write log 3' >> WriteToText(DIR_PATH + 'grouped_student_pcoll.txt')

    # remove duplicate student records
    distinct_student_pcoll = grouped_student_pcoll | 'Dedup student' >> beam.ParDo(DedupStudentRecordsFn())

    # write PCollection to log file
    distinct_student_pcoll | 'Write log 4' >> WriteToText(DIR_PATH + 'distinct_student_pcoll.txt')

    dataset_id = 'college_modeled'
    table_id = 'Student_Beam_DF'
    schema_id = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE,status:STRING'

    # write PCollection to new BQ table
    distinct_student_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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