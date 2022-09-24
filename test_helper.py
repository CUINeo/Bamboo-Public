import os, re
import numpy as np

# Test setting.
thread = 16

# TPCC test setting.
tpcc_output_folder = 'tpcc_result'
num_wh_list = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32]

# YCSB test setting.
ycsb_output_folder = 'ycsb_result'
theta_list = [round(x, 2) for x in list(np.arange(0.5, 1, 0.05))]

# Plot setting.
figure_folder = 'test_figure'

# MVCC, HEKATON and VLL cannot be compiled successfully right now.
cc_list = ['BAMBOO', 'WOUND_WAIT', 'NO_WAIT', 'WAIT_DIE', 'SILO', 'IC3', 'OCC', 'HSTORE', 'TICTOC']

def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, 'w')
    f.write(s)
    f.close()

def change_wl(wl):
    f = open('config.h', 'r+')
    for line in f:
        if '#define WORKLOAD' in line:
            replace('config.h', line, '#define WORKLOAD ' + wl + '\n')
            break
    f.close()

def change_cc(cc):
    f = open('config.h', 'r+')
    for line in f:
        if '#define CC_ALG' in line:
            replace('config.h', line, '#define CC_ALG ' + cc + '\n')
            break
    f.close()

def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, 'w')
    f.write(s)
    f.close()

def change_cc(cc):
    f = open('config.h', 'r+')
    for line in f:
        if '#define CC_ALG' in line:
            replace('config.h', line, '#define CC_ALG ' + cc + '\n')
            break
    f.close()

def compile():
    os.system('make clean > temp.out 2>&1')
    ret = os.system('make -j > temp.out 2>&1')
    if ret != 0:
        print('ERROR in compiling, output saved in temp.out')
        exit(0)
    else:
        os.system('rm -f temp.out')

def ycsb_execute(theta, output):
    os.system('./rundb -z' + str(theta) + ' > ' + output + ' 2>&1')

def tpcc_execute(num_wh, output):
    os.system('./rundb -n' + str(num_wh) + ' > ' + output + ' 2>&1')
