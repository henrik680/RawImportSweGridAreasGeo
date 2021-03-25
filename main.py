from __future__ import absolute_import
from google.cloud import datacatalog_v1, storage
from RenewalyticsDataCatalogLib import *
from RenewalyticsDataflowMetadataLib import *
import sys, argparse
from datetime import datetime
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

g_schema = {
    'fields': [{
        'name': 'geometry', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'NAMN', 'type': 'STRING', 'mode': 'REQUIRED'
    }]
}
project_name = 'RawImportSweGridAreasGeo'


class DataIngestion(beam.DoFn):
    def process(self, element):
        l_new = []
        for x in element["features"]:
            logging.info('DataIngestion.process: properties={}'.format(x['properties']))
            properties = x['properties']
            geo = {
                'geometry': str(x['geometry'])
            }
            d = {**properties, **geo}
            l_new.append(d)
        return l_new


def run(argv=None, save_main_session=True):
    logging.info("Starting {}".format(project_name))
    logging.info('argv={}'.format(argv))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-sample',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    logging.info('known_args: {}'.format(known_args))
    logging.info('pipeline_args: {}'.format(pipeline_args))

    bucket = known_args.input.split('/')[2]  # 'prod-bucket.renewalytics.io'
    blob = known_args.input[-len(known_args.input) + len('gs://') + len(bucket) +1:]
    metadata = {**{'code_module': project_name, 'input': known_args.input,
                   'output': known_args.output, 'updated': datetime.now()},
                **convert_storage_metadata_to_catalog(get_storage_metadata(storage.Client(), bucket, blob))}

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    (p

     | 'Read from a File' >> beam.io.ReadFromText(known_args.input)
     | 'Load JSON' >> beam.Map(json.loads)
     | 'Custom Parse' >> beam.ParDo(DataIngestion())
     | 'Write to BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    known_args.output,
                    schema=g_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Deletes all data in the BigQuery table before writing.
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))

    p.run().wait_until_finish()
    write_metadata(dc=datacatalog_v1.DataCatalogClient(), metadata=metadata, table_id=known_args.output.split('.')[-1])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
