
"""
State diagram: Note that do not keep track of the individual states
of the job instances, but just do some accounting when there is
failure.

register---->created---->inProgress------->submit----------->finished-->cleanout
 ^        |                         ^   |                 |
 |        |-->createFailure         |   |-->submitFailure |-->runFailure
 |                    |             |            |                    |
 ---------------------|             |-----<------|--------<-----------|

Conditions and exceptions:

submit --------> racer+1  if (retries+racers)<maxRetries)
can give: retryException,racerException

runFailure-----> retries+1,racer-1 if(racer>0 and retries < maxRetries)
can give:  retryException,racerException

submitFailure--> retries+1 if(retries < maxRetries)
can give: retryException

createFailure--> retries+1 if(retries < maxRetries)
can give: retryException

"""

import JobStateChangeAPI 
import JobStateInfoAPI 
