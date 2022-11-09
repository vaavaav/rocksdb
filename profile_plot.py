#!/usr/bin/env python3

import pandas as pd
import subprocess
import pathlib  
from matplotlib import pyplot
from numpy import arange
from dirsync import sync

# Configs
workspace = str(pathlib.Path.cwd())

## Scripts and executables
project_source_dir = f'{workspace}'
executable_dir = f'{workspace}/build'
scripts_dir = '.'
util_script = f'{scripts_dir}/util.sh'

results_dir = f'{workspace}/profiling_results'
db_backup = f'{workspace}/profile_db_backup'
db_dir    = f'{workspace}/profile_db'

## General settings for db_bench

settings = {}
settings['benchmarks'] = 'ycsbwklda'
settings['threads'] = 4 # number of foreground threads 
settings['duration'] = 500 # duration of a run (in seconds) 
settings['num'] = 2_000_000 # number of kvs  
settings['compression_type'] = 'none' 
settings['cache_size'] = 1<<30 # 1 GB 
settings['key_size'] = 16 
settings['value_size_distribution_type'] = 'fixed' 
settings['value_size'] = 1024 
settings['max_background_flushes'] = 1 
settings['max_background_compactions'] = 3 
settings['db'] = db_dir

# -------

def run(other_settings):
    global settings, executable_dir
    new_settings = {**settings, **other_settings}
    # Create command with overriding settings
    command = [f'{executable_dir}/db_bench'] + [f'--{k}={new_settings[k]}' for k in settings]
    # Run command
    subprocess.call(command)

def plot(profile, save_dir : str):
    for metric in profile['METRIC'].unique():
        df = pd.DataFrame(profile[profile['METRIC'] == metric], columns=profile.columns[1:])
        df.plot()
        pyplot.ylabel(metric)
        pyplot.title(metric)
        pyplot.savefig(f'{save_dir}/{metric}.png')

def mkBackup():
    global db_backup, db_dir
    print('Creating backup of the db directory')
    if pathlib.Path(db_backup).is_dir():
        pathlib.Path(db_backup).unlink()
    pathlib.Path(db_backup).mkdir(parents=True, exist_ok=True) 
    sync(db_dir, db_backup, 'sync')

def loadBackup():
    global db_backup, db_dir
    print('Loading backup into the db directory')
    sync(db_backup, db_dir, 'sync')


# ----

loading = { 
    'num' : 5_000,
    'benchmarks' : 'ycsbfill'
     }
setups = {}
setups['uniform'] = { 
    'YCSB_uniform_distribution' : 'true', 
    'use_existing_db' : 'true',
    'profiling_file' : f'{results_dir}/uniform.csv' 
    }
for i in arange(0.5,1.4,0.2): 
    setups['zipfian_'+str(i).replace('.','_')] = { 
         'YCSB_uniform_distribution' : 'false', 
         'use_existing_db' : 'true', 
         'zipf_coef' : i,
         }


# Plot config
pyplot.xlabel('time (s)')

# Loading
run(loading)
mkBackup()
for setup in setups:
    pathlib.Path(f'{results_dir}/{setup}').mkdir(parents=True,exist_ok=True) 
    setups[setup]['profiling_file'] = f'{results_dir}/{setup}/result.csv'
    loadBackup()
    run(setups[setup])
    profile = pd.read_csv(setups[setup]['profiling_file'])
    plot(profile, f'{results_dir}/{setup}')

    
