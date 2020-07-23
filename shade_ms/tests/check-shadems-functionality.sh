#! /bin/bash
# standard checks to run after editing to verify that the basic functionality will still succeed

export LC_NUMERIC=C
# set up indication colours (hack style test, but it works)
RED="\033[1;31m"
GREEN="\033[1;32m"
YELLOW="\033[0;33m"
NOCOLOR="\033[0m"


# simple input to provide input msfile
if [[ "$#" -lt 1 ]]
then
    # TODO: need to improve the unpacking of the input parameters
    echo "Usage: $0 <msfile> [-c|--clean] [-v|--verbose]"
    exit 1
fi
msfile=$1; shift
verbose=0
# handle optional arguments if they exist, ignore the rest
while [[ $# -gt 0 ]]
    do
    key=$1
    case $key in
        -c | --clean)
            # clean previous output
            make clean
            shift  # past argument
            ;;
        -v | --verbose)
            # show png output graphs
            verbose=1
            shift  # past argument
            ;;
        *)  # unknown options
            shift  # past unknown arguments
            ;;
    esac
done


# general function to run shadems commands
function runcmd {
    CMD="shadems $msfile $args"
    if [ -n "$figname" ]
    then
        CMD="$CMD --png $figname"
    fi
    echo $CMD

    if $CMD
        then
            echo -e "${GREEN} Success ${NOCOLOR}"
        else
            echo -e "${RED} Failure ${NOCOLOR}"
            if [[ $succeed == 1 ]]
            then
                exit 1
            fi
        fi
    echo
}


## base functionality following the README
## (all examples must always work)
## TODO: select or build an MS that contains one calibrator target, small array, wideband obs
ARGS=(
# default settings will produce a plot of amplitude vs time
# all fields, spectral windows, and correlation products will be selected
""
# change the plot axes
"--xaxis FREQ --yaxis DATA:phase"
# complex-valued columns
"--xaxis FREQ --yaxis DATA:amp:XX"
# axis selection arguments for multiple plots
"--xaxis TIME,CHAN --yaxis DATA:amp:XX,DATA:amp:YY"
# comma-separated list via the relevant argument allows for arbitrary data selection
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY"
# channel selection, a start[:stop][:step]
"--xaxis CHAN --yaxis DATA:amp --chan 10:21"
# Antenna selection with start[:stop][:step][,num] with multiple comma-separated slices
# assuming the smallest array will always have 4 antennas
"--xaxis CHAN --yaxis DATA:phase --ant-num 0:1,3"
# iteration
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY --iter-ant"
# colourisation
"--xaxis UV --yaxis DATA:amp:XX --colour-by ANTENNA1"
"--xaxis U --yaxis V --colour-by DATA:amp:XX --cmin 0 --cmax 5"
)
for args in "${ARGS[@]}"
do
    runcmd $msfile $args
done


# show output graphs
if [[ $verbose == 1 ]]
then
    echo "show generated output images"
    for file in *.png
    do
        echo $file
        xdg-open $file
    done
fi

# -fin-

