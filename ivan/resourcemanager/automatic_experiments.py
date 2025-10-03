from automatic_experiments_parameters import experiments_config

if __name__ == '__main__':
    for experiment_config in experiments_config.all_experiments_configurations():
        print(experiment_config)
