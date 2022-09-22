import os
import numpy as np
from test_helper import *

if __name__ == '__main__':
    output_folder = 'ycsb_result'
    theta_list = [round(x, 1) for x in list(np.arange(0, 1, 0.1))]

    if not os.path.isdir(output_folder):
        os.system('mkdir ' + output_folder)

    change_wl('YCSB')

    for cc in cc_list:
        change_cc(cc)
        compile()
        assert os.path.exists('rundb'), 'rundb does not exist.'
        for theta in theta_list:
            output_file = output_folder + '/' + cc + '_ycsb_' + str(theta) + '.txt'
            ycsb_execute(theta, output_file)
        print(cc + ' test done.')

    print('YCSB test done. Results stored in the ' + output_folder + ' folder.')
