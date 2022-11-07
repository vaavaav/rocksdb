#!/usr/bin/env python3

import pandas as pd
import subprocess
import os
from matplotlib import pyplot

# Execute db_bench

executable = './build/db_bench'
settings = {}
settings['benchmarks'] = 'ycsbwklda'
settings['threads'] = 4 
settings['num'] = 100000000
settings['compression_type'] = 'none'
settings['cache_size'] = 1<<30 
settings['key_size'] = 16 
settings['value_size_distribution_type'] = 'fixed' 
settings['value_size'] = 1024 
settings['max_background_flushes'] = 1 
settings['max_background_compactions'] = 4 

def mkCommand(other_settings):
    global settings, executable
    new_settings = {**settings, **other_settings}
    return [executable] + [f'--{k}={new_settings[k]}' for k in settings]


## Loading
loading_settings = {}
loading_settings['num'] = 50000000
loading_settings['benchmarkds'] = "ycsbfill"
subprocess.call(mkCommand(loading_settings))

runs = {}
runs['uniform'] = { 'YCSB_uniform_distribution' : 'true' }
runs['zipfian'] = { 'YCSB_uniform_distribution' : 'false' }


prof_dir = 'profiles'
os.mkdir(prof_dir)

for run in runs:
    # Run
    subprocess.call(mkCommand(runs[run]))
    # Plot
    os.mkdir(f'{prof_dir}/{run}') # make dir to store benchmark statistics 
    profile = pd.read_csv(r'trace_file.csv')
    for metric in profile['METRIC'].unique():
        df = pd.DataFrame(profile[profile['METRIC'] == metric], columns=profile.columns[1:])
        df.plot()
        pyplot.xlabel('seconds')
        pyplot.ylabel(metric)
        pyplot.savefig(f'{prof_dir}/{run}/{metric}.png')
