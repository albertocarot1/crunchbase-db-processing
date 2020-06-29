import multiprocessing
from subprocess import Popen


def main():
    max_concurr = multiprocessing.cpu_count() - 1

    configs = [f'min_words={el}' for el in range(2, 64, 1)]
    cmds_list = [['python', 'find_threshold.py', 'with', config] for config in configs]
    i = 0
    active_num = 0
    finished = 0
    spawn_procs = []
    print(f'Starting a total of {len(configs)} experiments...')
    while finished < len(cmds_list):

        if active_num < max_concurr and i < len(cmds_list):
            p = Popen(cmds_list[i])
            i += 1
            print(f'EXPERIMENTS STARTED: {i}')
            active_num += 1
            print(f'EXPERIMENTS RUNNING ATM: {active_num}')
            spawn_procs.append(p)

        to_del = []
        for ind, proc in enumerate(spawn_procs):
            if proc.poll() is not None:
                active_num -= 1
                finished += 1
                print(f'EXPERIMENTS COMPLETED: {finished}')
                to_del.append(ind)

        for j in to_del:
            del spawn_procs[j]
    print('Experiments done. :)')


if __name__ == '__main__':
    main()