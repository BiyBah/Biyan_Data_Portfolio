import numpy as np
import pandas as pd
import luigi
import os
import subprocess
import sqlalchemy

class Extract(luigi.Task):

    filename = luigi.Parameter()
    
    def requires(self):
        pass

    def run(self):
        df = pd.read_csv(self.filename)
        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/raw/csv.csv")

if __name__ == "__main__":
    luigi.build([Extract(filename = "filename.csv")], local_scheduler = True)