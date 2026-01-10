import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt libro.txt -> Descarga el archivo localmente

def run():
  # Pipeline configuration
  options = PipelineOptions(
    runner='DataflowRunner', # Use 'DirectRunner' for local execution and 'DataflowRunner' for cloud execution
    project='aesthetic-vent-483820-d5',
    region='us-central1',
    temp_location='gs://gcs-bucket-05/temp',
  )
  
  with beam.Pipeline(options=options) as p:
    (
      p
      | "Leer archivo" >> beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
      | "Separar palabras" >> beam.FlatMap(lambda line: line.split())
      | "Contar palabras" >> beam.combiners.Count.PerElement()
      | "Guardar palabras" >> beam.io.WriteToText('gs://gcs-bucket-05/wordcount/output')
    )
  print("Pipeline ejecutada con éxito.")
    
if __name__ == "__main__":
    run()