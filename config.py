import os.path
from configparser import ConfigParser

current_dir = os.path.dirname(os.path.abspath(__file__))
rel_file_path = os.path.join(current_dir, "database.ini")
abs_file_path = os.path.abspath(rel_file_path)


def config(filename: str = abs_file_path, section: str = "postgresql") -> dict:
    '''
    Получение данных из конфигурационного файла
    '''
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename))
    return db