import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class GroupHostListing(beam.DoFn):

    def process(self, element):
        # group host entity with the same (host_id) - PK of the table
        host_id = element['host_id']
        host_name=element['host_name']
        listings_count=element['listings_count']

        record = {'host_id': host_id, 'host_name': host_name, 'listings_count': listings_count}
        
        return [((host_id), record)]
    
class MakeUniqueHost(beam.DoFn):
    def process(self, element):
        h_id, host = element # host= _UnwindowedValues object
        host_list = list(host)
        
        # Rule:
        # For the same (host_id), sum the listings_count 
        host_name = host[0]['host_name']

        n = len(host_list)
        listings_count = 0
            
        for i in range(n):
            if host[i]['listings_count'] is not None:
                listings_count += host[i]['listings_count']
                
        record = {'host_id': h_id, 'host_name': host_name, 'listings_count': listings_count}
   
        return [record]
    
def run():
    PROJECT_ID = 'total-earth-236521'
    BUCKET = 'gs://finalpush-327e/temp'

    options = {
     'project': PROJECT_ID
     }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DirectRunner', options=opts)

    ## Work starts here
    sql = 'SELECT host_id, host_name, listings_count FROM datamart.host limit 100'
        
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
    query_results | 'Log BQ Read' >> WriteToText('bq_read_host.txt')

    # group by (host_id)

    host_pcoll = query_results | 'Group HostListing' >> beam.ParDo(GroupHostListing())
    
    grouped_host_pcoll = host_pcoll | 'GroupByKey' >> beam.GroupByKey()
    
    # Intermediate output for debugging
    grouped_host_pcoll | 'Log grouped by host_id' >> WriteToText('grouped_host_output.txt')
    
    # Make unique (host_id)
    unique_host_pcoll = grouped_host_pcoll | 'Make Unique Host' >> beam.ParDo(MakeUniqueHost())
    
    # write result to bq
    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'host_Beam'
    schema_id = 'host_id:INTEGER, host_name:STRING, listings_count:INTEGER'
    
    unique_host_pcoll | 'Log class unique' >> WriteToText('unique_host_output.txt')
    unique_host_pcoll | 'Write class to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
    
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
