import os
from test_helper import *

if __name__ == '__main__':
    if not os.path.isdir(delta_output_folder):
        os.system('mkdir ' + delta_output_folder)

    change_wl('YCSB')
    change_cc('DIRTY_OCC')

    for delta in delta_list:
        change_delta(delta)
        compile()
        assert os.path.exists('rundb'), 'rundb does not exist.'

        output_file = delta_output_folder + '/' + str(delta) + '.txt'
        execute(output_file)
        print(str(delta) + ' test done.')

    print('DELTA test done. Results stored in the ' + delta_output_folder + ' folder.')
