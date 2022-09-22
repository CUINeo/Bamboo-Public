import os, re
import numpy as np

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

def execute(theta, output):
    os.system('./rundb -z' + str(theta) + ' > ' + output + ' 2>&1')

if __name__ == '__main__':
    output_folder = 'ycsb_result'
    cc_list = ['BAMBOO', 'WOUND_WAIT', 'NO_WAIT', 'WAIT_DIE', 'SILO', 'IC3']
    theta_list = [round(x, 1) for x in list(np.arange(0, 1, 0.1))]

    if not os.path.isdir(output_folder):
        os.system('mkdir ' + output_folder)

    for cc in cc_list:
        change_cc(cc)
        compile()
        assert os.path.exists('rundb'), 'rundb does not exist.'
        for theta in theta_list:
            output_file = output_folder + '/' + cc + '_ycsb_' + str(theta) + '.txt'
            execute(theta, output_file)
        print(cc + ' test done.')

    print('YCSB test done. Results stored in the ' + output_folder + ' folder.')
