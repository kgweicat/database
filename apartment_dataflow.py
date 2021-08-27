import logging, datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class GroupHostListing(beam.DoFn):
    # group Apartment entity with the same (host_id, listing_id) - PK

    def process(self, element):
        listing_id = element['listing_id']
        host_id = element['host_id']
        name = element['name']
        room_type = element['room_type']
        city = element['city']
        price = element['price']
        min_night = element['minimum_nights']
        max_night = element['maximum_nights']
        availability = element['availability_365']
        number_of_reviews = element['number_of_reviews']
        last_review = element['last_review']
        reviews_per_month = element['reviews_per_month']

        record = {'host_id': host_id, 'listing_id': listing_id, 'name': name,'room_type': room_type, 'city': city, 'price':price, 'min_night': min_night,'max_night':max_night, 'availability':availability, 'number_of_reviews': number_of_reviews, 'last_review': last_review, 'reviews_per_month': reviews_per_month}
        
        return [((host_id,listing_id), record)]
    
class MakeUniqueApartment(beam.DoFn):
    # Make the unique apartment according to our numerical rules below
    def process(self, element):
        apartment_id, apartment = element # apartment= _UnwindowedValues object
        apt_list = list(apartment)
        
        # Rules:
        # For the same (listing_id, host_id):
        # sum the avalibility 365 (the data implies the host has many room in a unit)
        # -- the result may exceed 365
        # sum the reviews_per_month, number_of_reviews
        # min night = min (all of duplicated)
        # max night = max (all of duplicated)
        # Average the price across each duplicate
        # 
        
        # These attributes never change across the duplicate
        name = apt_list[0]['name']
        room_type = apt_list[0]['room_type']
        city = apt_list[0]['city']
        
        last_review = apt_list[0]['last_review']
        
        # apartment is a list of each (duplicated) record(dict)
        n = len(apt_list)
        total_avail = 0
        min_night = 365
        max_night = 0
        price = 0
        number_of_reviews = 0
        reviews_per_month = 0
        availability = 0
        nprice = n
        nrpm = n
            
        for i in range(n):
            if apt_list[i]['price'] is not None:
                price += apt_list[i]['price']
            else:
                nprice -= 1 # not account to averaging
            if apt_list[i]['price'] is not None:
                total_avail += apt_list[i]['availability']
            if apt_list[i]['number_of_reviews'] is not None:
                number_of_reviews += apt_list[i]['number_of_reviews']
            if apt_list[i]['reviews_per_month'] is not None:
                reviews_per_month += apt_list[i]['reviews_per_month']
            else:
                nrpm -= 1 # not account to averaging
            
            # extract the (global) minimum night
            if apt_list[i]['min_night'] is not None:
                if apt_list[i]['min_night'] < min_night:
                    min_night = apt_list[i]['min_night']
            # extract the (global) maximum night
            if apt_list[i]['max_night'] is not None:
                if apt_list[i]['max_night'] > max_night:
                    max_night = apt_list[i]['max_night']
                
        # average out the price
        price = price/nprice
        # average reviews_per_month
        if nrpm ==0 :
            reviews_per_month = None 
        else:
            reviews_per_month = reviews_per_month/nrpm
        
    
        record = {'host_id': apartment_id[0], 'listing_id': apartment_id[1], 'name': name,'room_type': room_type, 'city': city, 'price':price, 'min_night': min_night,'max_night':max_night, 'availability':total_avail, 'number_of_reviews': number_of_reviews, 'last_review': last_review, 'reviews_per_month': reviews_per_month}
   
        return [record]
    
    
def run():
    PROJECT_ID = 'total-earth-236521'
    BUCKET = 'gs://finalpush-327e'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='apartment',
    temp_location=BUCKET + '/temp',
    region='us-central1')

    p = beam.pipeline.Pipeline(options=options)

    ## Work starts here
    sql = 'SELECT listing_id, host_id, name, room_type, city, price, minimum_nights, maximum_nights, availability_365, number_of_reviews, last_review, reviews_per_month FROM datamart.apartment'
        
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
    query_results | 'Log BQ Read' >> WriteToText(DIR_PATH +'apartment_bq_read.txt')

    # group by (host_id, listing_id)

    apartment_pcoll = query_results | 'Group HostListing' >> beam.ParDo(GroupHostListing())
    
    grouped_apartment_pcoll = apartment_pcoll | 'GroupByKey' >> beam.GroupByKey()
    
    # Intermediate output for debugging
    grouped_apartment_pcoll | 'Log grouped by host_id, listing_id' >> WriteToText(DIR_PATH +'grouped_apartment_output.txt')
    
    # Make unique (host_id, listing_id)
    unique_apartment_pcoll = grouped_apartment_pcoll | 'Make Unique Apartment' >> beam.ParDo(MakeUniqueApartment())
    
    # write result to bq
    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'apartment_Dataflow'
    schema_id = 'host_id:INTEGER, listing_id:INTEGER,name:STRING, room_type:STRING, city:STRING,price:FLOAT,min_night:INTEGER,max_night:INTEGER, availability:INTEGER, number_of_reviews:INTEGER,last_review:DATE, reviews_per_month:FLOAT'
    

    unique_apartment_pcoll | 'Log class unique' >> WriteToText(DIR_PATH +'unique_apartment_output.txt')
    unique_apartment_pcoll | 'Write class to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
    
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
