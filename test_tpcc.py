import os
from test_helper import *

if __name__ == '__main__':
    if not os.path.isdir(tpcc_output_folder):
        os.system('mkdir ' + tpcc_output_folder)

    change_wl('TPCC')

    for cc in cc_list:
        change_cc(cc)
        compile()
        assert os.path.exists('rundb'), 'rundb does not exist.'
        for num_wh in num_wh_list:
            output_file = tpcc_output_folder + '/' + cc + '_tpcc_' + str(num_wh) + '.txt'
            tpcc_execute(num_wh, output_file)
        print(cc + ' test done.')

    print('TPCC test done. Results stored in the ' + tpcc_output_folder + ' folder.')
