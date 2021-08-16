from pyarrow import csv
from pyarrow import fs
import pyarrow as pa

table = csv.read_csv("fake.csv")

local = fs.LocalFileSystem()

with local.open_output_stream("fake.arrow") as file:
   with pa.RecordBatchFileWriter(file, table.schema) as writer:
      writer.write_table(table)
