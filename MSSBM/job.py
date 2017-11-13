import json
import sys
import os
import time

class work:
    
    def __init__(self, message,headers, m, c):
        self.message        = message
        self.headers        = headers
        self.instance         = getattr(__import__(m, fromlist=[c]) , c)()

    def run(self, bool):
        ''' '''
        # si RR on renvoi un message
        if bool:
            self.logprint()
            return self.instance.run()
        else:
            self.logprint()
            return self.instance.run()

    def logprint(self):
        ''' demande d'information sur le traitement'''
        sys.stdout.write("----  JOB \n")
        sys.stdout.write("PID Work...: "+str(os.getpid())+"\n")
        sys.stdout.write("MESSAGE...: "+str(self.message)+"\n")
        for i in self.headers:
            sys.stdout.write("headers["+i+"]="+self.headers[i]+"\n")
        sys.stdout.write("Message"+self.message+"\n")
        sys.stdout.flush()
