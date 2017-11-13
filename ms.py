from MSSBM.mservice_class import msService
import sys
import os
from time import sleep

if __name__ == '__main__':

    sys.stdout.write("PID MAIN MONIT...: "+str(os.getpid())+"\n")
    sys.stdout.flush()

    #AMQP_CONNEXION = msStart()
    MSERVICE = msService()

    while MSERVICE.isConnected():
        sleep(1)
