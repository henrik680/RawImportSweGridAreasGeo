from __future__ import absolute_import
import GcpDataCatalog   # Add GoogleDataCatalogLib folder to Preferences Project Structure
import argparse
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


class custom_json_parser(beam.DoFn):
    def process(self, element):
        l_new = []
        for x in element["features"]:
            properties = x['properties']
            geo = {
                'geometry': str(x['geometry'])
            }
            d = {**properties, **geo}
            l_new.append(d)
        return l_new


def run(argv=None, save_main_session=True):
    logging.info("Starting RawImportSweGridAreasGeo")
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

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    (p

     | 'Read from a File' >> beam.io.ReadFromText(known_args.input)
     | 'Load JSON' >> beam.Map(json.loads)
     | 'Custom Parse' >> beam.ParDo(custom_json_parser())
     | 'Write to BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    known_args.output,
                    schema=g_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Deletes all data in the BigQuery table before writing.
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
