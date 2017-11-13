from time import sleep
import time
import stomp
import uuid
import os
import configparser
import datetime
import sys
from multiprocessing import Process, Queue, TimeoutError
import multiprocessing
from MSSBM.job import work
import signal

class msListener(stomp.ConnectionListener):
    '''
        Le listener utiliser par la class msService
        A réception d'un message, il identifie s'il s'agit d'une communication en mode R/R en vérifiant la présence de la variable id_message_return dans le header.
        Si c'est le cas, il stocke le message dans la liste messagesRcv.
    '''

    def __init__(self,parent,*args, **kwargs):
        super(msListener, self).__init__(*args, **kwargs)
        sys.stdout.write("PID msListener...: "+str(os.getpid())+"\n")
        sys.stdout.flush()

        self.parent = parent
        self.messagesRcvRR = []

        self.num_processes = multiprocessing.cpu_count()*2
        self.pool = multiprocessing.Pool(processes=self.num_processes)
        self.processes = []
        sys.stdout.write("POOL PROCESS...: "+str(self.num_processes)+"\n")
        sys.stdout.flush()


    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):

        # Nous sommes dans le cas d'un RR ou nous attendons une réponse => présence du id_message_return sans queue de retour
        # On stocke le message dans la list
        if 'id_message_return' in headers and 'reply-to' not in headers:
            #on informe le monitorring de la réception du retour RR
            m = "{'MS_NAME':'"+self.parent.MS_NAME+"','MS_ID':'"+self.parent.MS_ID+"','COM':'RF', 'correlation-id':'"+headers['correlation-id']+"'}"
            self.parent.sendMonit(m)
            self.messagesRcvRR.append({'m':message,'h':headers,'id_message_return':headers['id_message_return']})
        else:
            # on recois un message
            #auquel on doit répondre (R/R)
            w = work(message,headers,self.parent.MS_MODULE_JOB, self.parent.MS_JOB)

            if 'reply-to' in headers:
                #on informe le monitorring de la réception d'un message pour du RR
                m = "{'MS_NAME':'"+self.parent.MS_NAME+"','MS_ID':'"+self.parent.MS_ID+"','COM':'RR', 'correlation-id':'"+headers['correlation-id']+"'}"
                self.parent.sendMonit(m)
                #proc = Process(target=w.run, args=(True,)).start()
                tic = round(time.time() * 1000)
                m = self.pool.apply_async(w.run, (True,))
                toc = round(time.time() * 1000)
                self.parent.sendFF(headers['reply-to'], m.get(timeout=1),headers['correlation-id'], headers['id_message_return'])
                self.parent.sendMonit("{'MS_NAME':'"+self.parent.MS_NAME+"','MS_ID':'"+self.parent.MS_ID+"','time':'"+str(toc-tic)+"','MS_FUNCTION':'function'}")
            #auquel aucune réponse n'est attendue (F/F)
            else:
                #on informe le monitorring de la réception d'un message pour du FF
                m = "{'MS_NAME':'"+self.parent.MS_NAME+"','MS_ID':'"+self.parent.MS_ID+"','COM':'FF', 'correlation-id':'"+headers['correlation-id']+"'}"
                self.parent.sendMonit(m)
                self.pool.apply_async(w.run, (False,))


    def getMessageRcvRR(self, id_message_return):
        '''
        Retrouve le mesasge stocké dans messagesRcvRR ayant comme id_message_return la valeur id_message_return
        '''
        for message in self.messagesRcvRR:
            if message['id_message_return'] == id_message_return:
                self.messagesRcvRR.remove(message)
                return message
        return 0

    def printInfo(self,headers, message):
        # on recois un message
        sys.stdout.write("-------------------->>  Listener \n"+self.parent.MS_NAME)
        sys.stdout.write("PID Work...: "+str(os.getpid())+"\n")
        sys.stdout.write("MESSAGE...: "+str(message)+"\n")
        for i in headers:
            sys.stdout.write("headers["+i+"]="+headers[i]+"\n")
        sys.stdout.write("Message >> "+message+"\n")
        sys.stdout.write("-------------------->>  Listener \n\n")
        sys.stdout.flush()


class msService(object):

    def __init__(self,fileConf='./etc/defaults.cfg'):

        self.FILE_ID         = './filegen/.id_srv'

        config = configparser.ConfigParser()
        config.readfp(open(fileConf))
        self.HOST_STOMP      = config.get('amqp','host_stomp')
        self.PORT_STOMP      = config.get('amqp','port_stomp')
        self.MONITORRING     = config.get('administration','monitorring')
        self.MANAGEMENT      = config.get('administration','management')
        self.MS_NAME         = config.get('administration','name_service')
        self.MS_TOPIC        = config.get('B2B','b2b_topic')
        self.MS_QUEUE        = config.get('B2B','b2b_queue')
        self.MS_EVT_PUBLISH  = config.get('B2B','b2b_topic_evt')
        self.MS_MODULE_JOB   = config.get('JOB','module')
        self.MS_JOB          = config.get('JOB','class')
        self.msListener      = msListener(self)

        # Etablissement de la connexion à activemq
        CONNEXION = []
        for h in self.HOST_STOMP.split(','):
            CONNEXION.append((h,self.PORT_STOMP))
        #print(*CONNEXION, sep='\n')

        try:
            self.AMQP_CONNEXION = stomp.Connection(host_and_ports=CONNEXION, keepalive=True, vhost=self.HOST_STOMP, heartbeats=(0, 0))
        except Exception:
            _, e, _ = sys.exc_info()
            sys.stdout.write("Unable to send heartbeat, due to: "+str(e))
            sys.stdout.flush()

        self.AMQP_CONNEXION.set_listener('MS'+self.MS_NAME, self.msListener)
        self.AMQP_CONNEXION.start()
        self.AMQP_CONNEXION.connect(username='',passcode='',wait=True)

        #
        # On vérifie si c'est une première instanciation en controlant l'existance du fichier FILE_ID qui contient l'id de l'instance du service
        #
        if ( os.path.isfile(self.FILE_ID) ):
            #ce n'est pas une première instanciation
            fichier = open(self.FILE_ID, "r")
            self.MS_ID = fichier.readline()
            fichier.close()
            self.sendMonit('{"STATUS": "2", "MS_NAME": "'+self.MS_NAME+'","MS_ID":"'+self.MS_ID+'","TIME":"'+str(datetime.datetime.now())+'"}')
        else:
            self.MS_ID = str(uuid.uuid1());
            fichier = open(self.FILE_ID, "w")
            fichier.write(str(self.MS_ID))
            fichier.close()
            self.sendMonit('{"STATUS": "1", "MS_NAME": "'+self.MS_NAME+'","MS_ID":"'+self.MS_ID+'","TIME":"'+str(datetime.datetime.now())+'"}')


        # la queue spécifique à cette instance de service. Permet de communiquer exclusivement avec cette instance (exemple en cas de R/R).
        self.MS_ID_QUEUE = self.MS_ID

        # on s'abonne
        self.AMQP_CONNEXION.subscribe(self.MS_QUEUE, id=self.MS_ID, ack='auto')
        self.AMQP_CONNEXION.subscribe(self.MS_TOPIC, id=self.MS_ID, ack='auto')
        self.AMQP_CONNEXION.subscribe(self.MS_ID_QUEUE, id=self.MS_ID, ack='auto')
        self.AMQP_CONNEXION.subscribe(self.MS_EVT_PUBLISH, id=self.MS_ID, ack='auto')
        self.AMQP_CONNEXION.subscribe(self.MANAGEMENT, id=self.MS_ID, ack='auto')

        sys.stdout.write("INSTANCE NAME...: "+self.MS_NAME+"\n")
        sys.stdout.write("INSTANCE ID...: "+self.MS_ID+"\n")
        sys.stdout.flush()

        signal.signal(signal.SIGINT, self.signal_handler)


    def sendRR(self, q, message, t=100, correlationId=str(uuid.uuid4())):
        ''' '''

        id_message_return = str(uuid.uuid4())

        try:
            self.AMQP_CONNEXION.send(q,message,headers={'reply-to': self.MS_ID_QUEUE,'id_message_return':id_message_return,'correlation-id':correlationId,'sender_name':self.MS_NAME,'sender_id':self.MS_ID})
            # On informe le monitorring de l'envoi d'un message RR
            message = "{'MS_NAME':'"+self.MS_NAME+"','MS_ID':'"+self.MS_ID+"','COM':'R', 'SEND_TO_RCP':'"+q+"','correlation-id':'"+correlationId+"'}"
            self.sendMonit(message)
        except Exception as e:
            sys.stdout.write("Object(msService).sendRR error...: "+message+"  -->  "+q+"\n")
            sys.stdout.flush()
            return 0

        b = True
        start_time = time.time()

        message = 0
        while not message :
            message = self.msListener.getMessageRcvRR(id_message_return)
            if (time.time() - start_time)*1000 > t:
                sys.stdout.write("RR "+message['id_message_return'] +"ELAPSED TIME timeout \n")
                sys.stdout.flush()
                return 0
        sys.stdout.write("RR "+message['id_message_return'] +"ELAPSED TIME:"+str((time.time() - start_time)*1000)+" ms  :: "+message['m']+"\n")
        sys.stdout.flush()

        return 1


    def sendFF(self, q, message, correlationId=str(uuid.uuid4()), id_message_return=""):
        ''' '''

        try:
            if id_message_return != "":
                self.AMQP_CONNEXION.send(q,message, headers={'correlation-id':correlationId,'sender-name':self.MS_NAME,'sender-id':self.MS_ID, 'id_message_return':id_message_return})
            else:
                self.AMQP_CONNEXION.send(q,message, headers={'correlation-id':correlationId,'sender-name':self.MS_NAME,'sender-id':self.MS_ID})
            # On informe le monitorring
            message = "{'MS_NAME':'"+self.MS_NAME+"','MS_ID':'"+self.MS_ID+"','COM':'F', 'SEND_TO_RCP':'"+q+"','correlation-id':'"+correlationId+"'}"
            self.sendMonit(message)
        except Exception as e:
            sys.stdout.write("Object(msService).sendFF error...: "+message+"  -->  "+q+"\n")
            sys.stdout.flush()
            return 0


    def sendMonit(self,message):
        ''' '''

        try:
            self.AMQP_CONNEXION.send(self.MONITORRING,message)
        except Exception as e:
            sys.stdout.write("Object(msService).sendMonit error...: "+message+"  -->  "+q+"\n")
            sys.stdout.flush()
            return 0


    def isConnected(self):
        return self.AMQP_CONNEXION.is_connected()

    def signal_handler(self,signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)
