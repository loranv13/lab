import json
import sys
import os
import time

class work:
    def __init__(self, message,headers, msService):
        self.message        = message
        self.headers        = headers
        self.msService      = msService
        #self.jsonMessage    = json.loads(message)

    def run(self, bool):
        ''' '''

        # Pour mesurer le temps de traitement
        tic = round(time.time() * 1000)

        # si RR on renvoi un message avec id_message_return renseigné
        if bool:
            #self.logprint()
            message = "{'retourAuth':'ok'}"
            self.msService.sendFF(self.headers['reply-to'], message,self.headers['correlation-id'], self.headers['id_message_return'])
        else:
            #self.logprint()
            return 1

        # fin du traitement
        toc = round(time.time() * 1000)

        # On envoi au monitorring les informations nécessaires
        # Nom du traitement, Temps de traitement
        message = "{'MS_NAME':'"+self.msService.MS_NAME+"','MS_ID':'"+self.msService.MS_ID+"','time':'"+str(toc-tic)+"','MS_FUNCTION':'function'}"
        self.msService.sendMonit(message)



    def logprint(self):
        ''' demande d'information sur le traitement'''
        sys.stdout.write("----  JOB \n")
        sys.stdout.write("PID Work...: "+str(os.getpid())+"\n")
        sys.stdout.write("MESSAGE...: "+str(self.message)+"\n")
        for i in self.headers:
            sys.stdout.write("headers["+i+"]="+self.headers[i]+"\n")
        sys.stdout.write("Message"+self.message+"\n")
        sys.stdout.flush()
