import math

def convertSeconds(remainingSeconds):

    if remainingSeconds<0:
        raise ProdException("seconds for conversion should be larger than 1")

    secondsInHours=3600
    secondsInMinutes=60

    hours=int(math.floor(float(remainingSeconds)/float(secondsInHours)))
    remainingSeconds=remainingSeconds-secondsInHours*hours
    minutes=int(math.floor(float(remainingSeconds)/float(secondsInMinutes)))
    seconds=remainingSeconds-secondsInMinutes*minutes

    timeFormat=str(hours)+":"+str(minutes)+":"+str(seconds)
    return timeFormat
