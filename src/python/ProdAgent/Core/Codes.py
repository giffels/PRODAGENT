
# this will be a dictionary of error/exceptions codes

exceptions={3000:'No result in local last service call table with a certain component ',\
            3001:'Error registering job. You probably are trying to register a job using an job id/job name that has already been used for registration ',\
            3002:'Trigger does not exists or duplicate action assigned ',\
            3003:'Trigger,jobID or flagID do not exists ',\
            3004:'Unknown error, please contact developers (Dave Evans, evansde@fnal.gov, Frank van Lingen (fvlingen@caltech.edu) or Carlos Kavka ',\
            3005:'Racers should not be smaller than 1 ',\
            3006:'Retries should not be smaller than 1 ',\
            3007:'Illegal transistion. Transition should be from "register" to "create" ',\
            3008:'Illegal transistion. Transition should be from "create" to "inProgress" ',\
            3009:'This jobID is not present in the database ',\
            3010:'Illegal transition. Transitions should be from "inProgress" ',\
            3011:'Reached maximum number of retries (this includes running jobs) ',\
            3012:'Illegal state transition to create. State is: ',\
            3013:'Reached the maximum number of retries: ',\
            3014:'SubmitFailure failed, please try again ',\
            3015:'A job with this ID is already submitted ',\
            3016:'Negative number of racers. Will not update ',\
            3017:'Runfailure failed. Please try again. ',\
            3018:'Illegal transistion. Transition should be inProgress ',\
            3019:'JobSpec ID does not exist ',\
            3020:'Job with this jobID has no jobs running yet ',\
            3021:'No message service associated to File object ',\
       }
