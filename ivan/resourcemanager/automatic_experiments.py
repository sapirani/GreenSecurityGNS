from ivan.resourcemanager.automatic_experiments_parameters import experiments_config

# TODO: SUPPORT SEQUENTIAL MODE AND PARALLEL MODE
# TODO: SUPPORT STARTING THE SCANNER AND STOPPING IT WHEN SCAN IS FINISHED
# TODO: SUPPORT STOPPING THE SCANNER UPON RECEIVING A KILL SIGNAL
# TODO: SUPPORT A SESSION ID BASED ON THE RECEIVED INPUTS FROM THE USER

if __name__ == '__main__':
    for experiment_config in experiments_config.all_experiments_configurations():
        print(experiment_config)
