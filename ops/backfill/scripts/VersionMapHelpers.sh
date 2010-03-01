
UpdateVersionMap()
{
    declare -i index=1    

    for key in ${VersionKeys[@]}
    do
        if [ "$key" == "$1" ]
        then
            let "VersionValues[$index] += 1"
            processing_version=${1}_backfill-v${VersionValues[$index]}
            return
        fi
        let "index += 1"
    done

    VersionKeys[$index]=$1
    VersionValues[$index]=1

    processing_version=${1}_backfill-v${VersionValues[$index]}
}

ReadVersionMap()
{
    declare -i index=1

    while read line
    do
        VersionKeys[$index]=${line%:*}
        VersionValues[$index]=${line#*:}
        let "index += 1"
    done < ~/backfill/etc/VersionStatus

    for key in ${VersionKeys[@]}
    do
        if [ "$key" == "$1" ]
        then
            return
        fi
    done

    let "index += 1"

    VersionKeys[$index]=$1
    VersionValues[$index]=0
}

WriteVersionMap()
{
    declare -i index=1
    for key in ${VersionKeys[@]}
    do
        if [ $index -eq 1 ]
        then
            echo "$key:${VersionValues[$index]}" > ~/backfill/etc/VersionStatus
        else
            echo "$key:${VersionValues[$index]}" >> ~/backfill/etc/VersionStatus
        fi
        let "index += 1"
    done
}


