import csv
import shutil
import os

class ResultCSV(object):
    def __init__(self):
        self._results = {}
        self._res_csvfile = None
        self._results_write = None

    def create_result_folder(self, resFolder):
        if os.path.exists(resFolder):
            try:
                shutil.rmtree(resFolder)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))
        if not os.path.exists(resFolder):
            os.mkdir(resFolder)

    def create_result_file(self, resFolder, filename='result.csv', header_stats = 'second,epoch time,GHI,forecast time,Forecast GHI'):
        header_list = header_stats.split(',')

        filename = os.path.join(resFolder, filename)
        try:
            os.remove(filename)
        except:
            pass
        for key in header_list:
            self._results[key] = 0.
        self._res_csvfile = open(filename, 'a')
        self._results_writer = csv.DictWriter(self._res_csvfile, fieldnames=header_list, delimiter=',',
                                              quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._results_writer.writeheader()
        return self._results

    def set_results(self, dict_value):
        self._results = dict_value

    def write(self, dict_value):
        self._results = dict_value
        self._results_writer.writerow(self._results)
        self._res_csvfile.flush()

    def close(self):
        self._res_csvfile.close()

