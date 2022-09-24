import os, sys
from test_helper import *
import matplotlib.pyplot as plt

def parse_filename(filename, wl):
    components = filename.split('_')
    if components[0] in cc_list:
        cc = components[0]
    else:
        cc = components[0] + '_' + components[1]

    arg = components[-1][:-4]
    if wl == 'tpcc':
        return cc, int(arg)
    elif wl == 'ycsb':
        return cc, float(arg)

    print('Invalid workload.')
    return cc, float(arg)

def plot_tpcc():
    assert os.path.isdir(tpcc_output_folder)
    if not os.path.isdir(figure_folder):
        os.mkdir(figure_folder)

    # Construct the result dictionary, containing lists of number of warehouses,
    # throughput and abort rate.
    res = {}
    for cc in cc_list:
        res[cc] = [num_wh_list, [-1] * len(num_wh_list), [-1] * len(num_wh_list)]

    # Traverse the output folder and parse output files.
    for root, _, files in os.walk(tpcc_output_folder):
        for filename in files:
            cc, num_wh = parse_filename(filename, 'tpcc')
            file = os.path.join(root, filename)
            f = open(file, 'r')
            for line in f:
                if '[summary]' in line:
                    # Get throughput, txn_cnt and abort_cnt from the summary line.
                    idx = line.find('=') + 1
                    commaidx = line.find(',', idx)
                    throughput = float(line[idx:commaidx])
                    idx = line.find('=', commaidx) + 2
                    commaidx = line.find(',', idx)
                    txn_cnt = float(line[idx:commaidx])
                    idx = line.find('=', commaidx) + 2
                    commaidx = line.find(',', idx)
                    abort_cnt = float(line[idx:commaidx])

                    # Compute abort rate.
                    abort_rate = abort_cnt / (txn_cnt + abort_cnt)

                    # Store throughput and abort_rate in res.
                    res_idx = num_wh_list.index(num_wh)
                    res[cc][1][res_idx] = throughput
                    res[cc][2][res_idx] = abort_rate

                    f.close()
                    break

    # Plot and save the throughput figure.
    _, ax = plt.subplots()
    ax.set_title('Throughput (' + str(thread) + ' threads)')
    for cc in cc_list:
        ax.plot(res[cc][0], res[cc][1], label=cc)
    ax.legend()
    ax.set(xlabel='Warehouse', ylabel='Throughput')
    ax.set_xticks(num_wh_list)
    figure_name = figure_folder + '/tpcc_' + str(thread) + '_throughput.png'
    plt.savefig(figure_name)

    # Plot and save the abort rate figure.
    _, ax = plt.subplots()
    ax.set_title('Abort rate (' + str(thread) + ' threads)')
    for cc in cc_list:
        ax.plot(res[cc][0], res[cc][2], label=cc)
    ax.legend()
    ax.set(xlabel='Warehouse', ylabel='Abort rate')
    ax.set_xticks(num_wh_list)
    figure_name = figure_folder + '/tpcc_' + str(thread) + '_abort_rate.png'
    plt.savefig(figure_name)

    print('TPCC plot done. Result figures stored in the ' + figure_folder + ' folder.')

def plot_ycsb():
    assert os.path.isdir(ycsb_output_folder)
    if not os.path.isdir(figure_folder):
        os.mkdir(figure_folder)

    # Construct the result dictionary, containing lists of thetas,
    # throughput and abort rate.
    res = {}
    for cc in cc_list:
        res[cc] = [theta_list, [-1] * len(theta_list), [-1] * len(theta_list)]

    # Traverse the output folder and parse output files.
    for root, _, files in os.walk(ycsb_output_folder):
        for filename in files:
            cc, theta = parse_filename(filename, 'ycsb')
            file = os.path.join(root, filename)
            f = open(file, 'r')
            for line in f:
                if '[summary]' in line:
                    # Get throughput, txn_cnt and abort_cnt from the summary line.
                    idx = line.find('=') + 1
                    commaidx = line.find(',', idx)
                    throughput = float(line[idx:commaidx])
                    idx = line.find('=', commaidx) + 2
                    commaidx = line.find(',', idx)
                    txn_cnt = float(line[idx:commaidx])
                    idx = line.find('=', commaidx) + 2
                    commaidx = line.find(',', idx)
                    abort_cnt = float(line[idx:commaidx])

                    # Compute abort rate.
                    abort_rate = abort_cnt / (txn_cnt + abort_cnt)

                    # Store throughput and abort_rate in res.
                    res_idx = theta_list.index(theta)
                    res[cc][1][res_idx] = throughput
                    res[cc][2][res_idx] = abort_rate

                    f.close()
                    break

    # Plot and save the throughput figure.
    _, ax = plt.subplots()
    ax.set_title('Throughput (' + str(thread) + ' threads)')
    for cc in cc_list:
        ax.plot(res[cc][0], res[cc][1], label=cc)
    ax.legend()
    ax.set(xlabel='Warehouse', ylabel='Throughput')
    ax.set_xticks(theta_list)
    figure_name = figure_folder + '/ycsb_' + str(thread) + '_throughput.png'
    plt.savefig(figure_name)

    # Plot and save the abort rate figure.
    _, ax = plt.subplots()
    ax.set_title('Abort rate (' + str(thread) + ' threads)')
    for cc in cc_list:
        ax.plot(res[cc][0], res[cc][2], label=cc)
    ax.legend()
    ax.set(xlabel='Warehouse', ylabel='Abort rate')
    ax.set_xticks(theta_list)
    figure_name = figure_folder + '/ycsb_' + str(thread) + '_abort_rate.png'
    plt.savefig(figure_name)

    print('YCSB plot done. Result figures stored in the ' + figure_folder + ' folder.')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        # Plot both tpcc and ycsb results.
        plot_tpcc()
        plot_ycsb()
    elif sys.argv[1] == 'tpcc':
        plot_tpcc()
    elif sys.argv[1] == 'ycsb':
        plot_ycsb()
    else:
        print('Invalid argument.')
