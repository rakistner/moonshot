import numpy as np
import queue
from queue import PriorityQueue
import threading
from threading import Thread
from fuzzywuzzy import fuzz
import csv

#=======================================
# class: Matcher type: Object
# Takes 2 data frames as arguments
# Takes rows of one data frame, and compares it to rows of another
# return top match(es) score (ex. 0.752349), index of top match(es) in Patent DataFrame
#
# Clinical Trials DataFrame Dictionary:
#   0 - NCT_ID
#   1 - OFFICIAL_TITLE
#   2 - START_DATE
#   3 - COMPLETION_DATE
#   4 - SOURCE
#   5 - INTERVENTION_TYPE
#
# Patent DataFrame Dictionary:
#   0 - Patent_or_Publication_ID
#   1 - Patent_Title
#   2 - Filing_Date
#   3 - Grant_or_Publication_Date
#   4 - NIH_Grant_Recipient_Organization
#   5 - Inventive_Type
#=======================================
class Matcher(object):
    
    def __init__(self, df, df2):
        super().__init__()
        self.queue = PriorityQueue(maxsize=0)
        self.numthreads = 8
        self.active = True #for shutting down threads cleanly
        self.trials_df = df
        self.patents_df = df2
        self.results_queue = queue.Queue()

        #initialize the worker threads for processing
        for i in range(self.numthreads):
            worker = Thread(target = self.__process__, args=())
            worker.start()

        for i in range(0, len(self.df2)):
            self.queue_job(i)

        self.queue.join()

        while not self.result_queue.empty():
            indexes, matches = self.results_queue.get_nowait()
            output = map(('{},{}').format(indexes, matches))
            with open('clincial_trials_matched.csv', 'w') as fp:
                a = csv.writer(fp, delimiter=',')
                a.writerows(output)
        
    def __process__(self):
        while self.active:
            try:

                # timeout in seconds, prevents spinlock on shutdown
                index = self.queue.get(timeout=10.0)[1]

                # do stuff with the data
                #========================
                df = self.trials_df.copy()
                df2 = self.patents_df.copy()
                self.process_row(index, df, df2)
                del df
                del df2
                print(index)
                #========================

                self.queue.task_done()
            except queue.Empty:
                break #timeout occured, try again

            #print("Shutting down thread: ", threading.current_thread())
            #print('done')

    def queue_job(self, index):
        if index is None: return
        self.queue.put((0,index))

    def process_row(self, index, df, df2):
        # print(type(index)) -> <'python.int'>
        df = df[ (df['Filing_Date'] < df2.ix[index,'START_DATE']) & (df['Grant_or_Publication_Date'] > df2.ix[index,'START_DATE']) & (df2.ix[index,'COMPLETION_DATE'] - df2.ix[index,'START_DATE'] > 0) ]
        if len(df) > 0:
            df['Title_Match_Potential'] = df['Patent_Title'].map( lambda x: fuzz.token_set_ratio(x, df2.ix[index,'OFFICIAL_TITLE']) if df2.ix[index,'OFFICIAL_TITLE'] is not None else None )
            df['Source_NIH_Match_Potential'] = df['NIH_Grant_Recipient_Organization'].map( lambda x: self.if_string_contains(x, index, df2, 'SOURCE') if (x and df2.ix[index,'SOURCE']) is not None else None )
            df['Source_FDA_Match_Potential'] = df['FDA_Applicant'].map( lambda x: self.if_string_contains(x, index, df2, 'SOURCE' )if (x and df2.ix[index,'SOURCE']) is not None else None )
            df['Inventive_Match_Potential'] = df['Inventive_Type'].map( lambda x: self.if_string_contains(x, index, df2, 'INTERVENTION_TYPE' )if (x and df2.ix[index,'INTERVENTION_TYPE']) is not None else None )
            df['Score'] = df[['Title_Match_Potential','Source_NIH_Match_Potential','Source_FDA_Match_Potential','Inventive_Match_Potential']].sum(axis=1)
            df['Score'] = df['Score'].apply( lambda x: float((x/5)/100) )
            all_scores = df['Score'].values
            top = max(all_scores)
            indexes = [i for i, j in enumerate(all_scores) if j == top]
            matches = df.ix[indexes]
            matches_ids = matches['Patent_or_Publication_ID'].values
            self.results_queue.put((top, matches_ids))
        else:
            return None

    def if_string_contains(self, x, index, df2, key):
        if x and index and key:
            if isinstance(x, str) and ';' in x:
                terms = x.split(';')
                p = []
                for term in terms:
                    if term in df2.ix[index, key]:
                        p.append(True)
                    else:
                        p.append(False)
                if True in p:
                    return 100
                else:
                    return 0
            else:
                return 0
        else:
            raise ValueError

    
#    if __name__ == '__main__':
#
#        self.results_queue = queue.Queue()
#
#        #initialize the worker threads for processing
#        for i in range(self.numthreads):
#            worker = Thread(target = self.__process__, args=())
#            worker.start()
#
#        for i in range(0, len(self.df2)):
#            self.queue_job(i)
#
#        self.queue.join()
#
#        while not self.result_queue.empty():
#            indexes, matches = self.results_queue.get_nowait()
#            print('{} - {}'.format(indexes, matches))