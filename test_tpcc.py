import os
from test_helper import *

if __name__ == '__main__':
    output_folder = 'tpcc_result'
    num_wh_list = [1, 2, 4, 8, 16, 32]

    if not os.path.isdir(output_folder):
        os.system('mkdir ' + output_folder)

    change_wl('TPCC')

    for cc in cc_list:
        change_cc(cc)
        compile()
        assert os.path.exists('rundb'), 'rundb does not exist.'
        for num_wh in num_wh_list:
            output_file = output_folder + '/' + cc + '_tpcc_' + str(num_wh) + '.txt'
            tpcc_execute(num_wh, output_file)
        print(cc + ' test done.')

    print('TPCC test done. Results stored in the ' + output_folder + ' folder.')
