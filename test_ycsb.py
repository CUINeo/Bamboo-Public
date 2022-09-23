import os
from test_helper import *

if __name__ == '__main__':
    if not os.path.isdir(ycsb_output_folder):
        os.system('mkdir ' + ycsb_output_folder)

    change_wl('YCSB')

    for cc in cc_list:
        change_cc(cc)
        compile()
        assert os.path.exists('rundb'), 'rundb does not exist.'
        for theta in theta_list:
            output_file = ycsb_output_folder + '/' + cc + '_ycsb_' + str(theta) + '.txt'
            ycsb_execute(theta, output_file)
        print(cc + ' test done.')

    print('YCSB test done. Results stored in the ' + ycsb_output_folder + ' folder.')
