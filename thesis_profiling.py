#!/usr/bin/env python3

import pandas as pd
import subprocess
import os
import shutil
from matplotlib import pyplot, rcParams

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
zipfian_coeficients = [0.4, 0.6, 0.8, 0.99, 1.2, 1.4]

# Default settings for db_bench
settings = {
    'benchmarks' : 'ycsbwklda',
    'threads' : 4,
    'duration' : 500,
    'num' : 100_000_000,
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

# Loading settings
loading = { 
    'num' : 50_000_000,
    'benchmarks' : 'ycsbfill',
    'duration' : 20,
    'profile' : 'false',
    'report_interval_seconds' : 0,
    'use_existing_db' : 'false'
}
# Setups
setups = {}
## Uniform distribution
setups['uniform'] = {
    'YCSB_uniform_distribution' : 'true',
    'profiling_results_dir' : f'{results_dir}/uniform'
}
## Zipfian distribution
for i in zipfian_coeficients: 
    setups[f'zipfian {i}'] = { 
         'zipf_coef' : i,
        'profiling_results_dir' : f'{results_dir}/zipfian_' + str(i).replace('.','_')
}
## Reject compactions (with previous distributions)
for setup in list(setups.keys()):
    setups[f'{setup} without compactions'] = {**setups[setup], **{
        'reject_compactions' : 'true',
        'profiling_results_dir' : f'{setups[setup]["profiling_results_dir"]}_zc'
    }}

# Plot settings
rcParams['figure.figsize'] = [15, 5]
rcParams['grid.color'] = '#888888'
rcParams['grid.alpha'] = '0.4'
rcParams['axes.grid'] = True 
rcParams['savefig.format'] = 'png'

# ------- 

def runDBBench(other_settings):
    new_settings = {**settings, **other_settings} # adding and overriding global settings with other settings
    command = [f'{executable_dir}/db_bench'] + [f'--{k}={new_settings[k]}' for k in new_settings] # generating command to run db_bench
    print('Running db_bench')
    print('Command: ' + ' '.join(command))
    subprocess.call(command) # run command

def plot(setup : str, profiling_results_dir : str):
    print(f'[PLOT] Generating plots for "{setup}"')
    profile = pd.read_csv(f'{profiling_results_dir}/results.csv')
    for metric in profile['METRIC'].unique():
        print(f'[PLOT:{setup}] Generating plot of {metric}')
        df = pd.DataFrame(profile[profile['METRIC'] == metric], columns=['compaction','foreground','global'])
        df.dropna(axis=1, how='all', inplace=True) 
        df.reset_index(drop=True, inplace=True)
        df.plot()
        pyplot.xlabel('time (s)')
        pyplot.ylabel(metric.replace('_', ' '))
        pyplot.title(setup)
        pyplot.savefig(f'{profiling_results_dir}/{metric}', bbox_inches='tight')
        pyplot.xlim([0,100])
        pyplot.savefig(f'{profiling_results_dir}/{metric}_zoomin', bbox_inches='tight')
        pyplot.close()

# --- Loading optimizations  

def saveBackup():
    print('Creating backup of the db directory')
    if os.path.exists(db_backup):
        shutil.rmtree(db_backup)
    shutil.copytree(db_dir, db_backup)

def loadBackup():
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
    os.makedirs(setups[setup]['profiling_results_dir'], exist_ok=True)
    loadBackup() # always load db backup before running
    subprocess.call([util_script, 'clean-heap'])
    runDBBench(setups[setup])
    plot(setup, setups[setup]['profiling_results_dir'])