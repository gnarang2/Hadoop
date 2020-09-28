import logging

class logger:
    def __init__(self,logger_name = '__name__', file_name = 'log/logs.log'):

        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        f_handler = logging.FileHandler(file_name)
        f_handler.setLevel(logging.DEBUG)

        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)

        self.logger.addHandler(f_handler)

if __name__ == "__main__":

    logger = logger(file_name='testing_log').logger
    logger.debug('logger instantiated')