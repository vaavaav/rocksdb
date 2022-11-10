#!/usr/bin/env python3

import pandas as pd
import subprocess
import os
import shutil
from matplotlib import pyplot
from numpy import arange

# Configs
workspace = os.getcwd() 

project_source_dir = f'{workspace}'
executable_dir = f'{workspace}/build'
scripts_dir = '.'
util_script = f'{scripts_dir}/utils.sh'

results_dir = f'{workspace}/profiling_results'
db_dir    = f'{workspace}/.profiling_db'
db_backup = f'{db_dir}_backup'
page_cache_size = '2' # GB

## Default settings for db_bench

settings = {
    'benchmarks' : 'ycsbwklda',
    'threads' : 4,
    #'duration' : 500,
    'num' : 200_000,
    'compression_type' : 'none',
    'cache_size' : 1<<30, # in bytes
    'key_size' : 16,
    'value_size_distribution_type' : 'fixed',
    'value_size' : 1024,
    'max_background_flushes' : 1,
    'max_background_compactions' : 3,
    'db' : db_dir,
    'report_interval_seconds' : 1,
    'profile' : 'true',
    'use_existing_db' : 'true'
} 

setups = {}
loading = { 
    'num' : 200_000,
    'benchmarks' : 'ycsbfill',
    'profile' : 'false',
    'report_interval_seconds' : 0,
    'use_existing_db' : 'false'
}
setups['uniform'] = { 
    'YCSB_uniform_distribution' : 'true'
}
for i in [0.4,0.6,0.8,0.99,1.2,1.4]: 
    setups['zipfian_'+str(i).replace('.','_')] = { 
         'zipf_coef' : i
}


# ------- 

def runDBBench(other_settings):
    global settings, executable_dir
    new_settings = {**settings, **other_settings}
    # Create command with overriding settings
    command = [f'{executable_dir}/db_bench'] + [f'--{k}={new_settings[k]}' for k in new_settings]
    # Run command
    print('Running db_bench')
    print('Command: ' + ' '.join(command))
    subprocess.call(command)

def plot(setup : str, profiling_results_dir : str):
    # my profiling
    profile = pd.read_csv(f'{profiling_results_dir}/results.csv')
    for metric in profile['METRIC'].unique():
        df = pd.DataFrame(profile[profile['METRIC'] == metric], columns=profile.columns[2:])
        df.dropna(axis=1, how='all', inplace=True) 
        df.reset_index(drop=True, inplace=True)
        df.plot()
        pyplot.xlabel('time (s)')
        pyplot.title(setup)
        pyplot.ylabel(metric)
        pyplot.savefig(f'{profiling_results_dir}/{metric}.png')
    # rocksdb simple profiling (qps)
    rdb_profile = pd.read_csv(f'{profiling_results_dir}/report.csv')
    rdb_profile.rename(columns = {'interval_qps' : 'global'}, inplace=True)
    df = pd.DataFrame(rdb_profile, columns=rdb_profile.columns[1:])
    df.plot()
    pyplot.xlabel('time (s)')
    pyplot.title(setup)
    pyplot.ylabel('queries')
    pyplot.savefig(f'{profiling_results_dir}/queries.png')



# --- Loading optimizations  

def saveBackup():
    global db_backup, db_dir
    print('Creating backup of the db directory')
    if os.path.exists(db_backup):
        shutil.rmtree(db_backup)
    shutil.copytree(db_dir, db_backup)

def loadBackup():
    global db_backup, db_dir
    print('Loading backup into the db directory')
    if os.path.exists(db_dir):
        shutil.rmtree(db_dir)
    shutil.copytree(db_backup, db_dir)


# Main execution
if os.path.exists(results_dir):
    shutil.rmtree(results_dir)

subprocess.call([util_script, 'limit-mem', f'{page_cache_size}G'])
runDBBench(loading) # load phase
saveBackup()          # store db entries in a backup
for setup in setups:
    setups[setup]['profiling_results_dir'] = f'{results_dir}/{setup}'
    setups[setup]['report_file'] = f'{results_dir}/{setup}/report.csv'# rocksdb simple profiling (qps)
    os.makedirs(setups[setup]['profiling_results_dir'], exist_ok=True)
    loadBackup() # always load db backup before running
    subprocess.call([util_script, 'clean-heap'])
    runDBBench(setups[setup])
    plot(setup, setups[setup]['profiling_results_dir'])