#! /bin/bash
# standard checks to run after editing to verify that the basic functionality will still succeed

export LC_NUMERIC=C
# set up indication colours (hack style test, but it works)
RED="\033[1;31m"
GREEN="\033[1;32m"
YELLOW="\033[0;33m"
NOCOLOR="\033[0m"

USAGE="Usage: $0 <msfile> [-c|--clean] [-v|--verbose] [-e|--parse_error] [-x|--extended]"
CLEAN=0
# simple input to provide input msfile
if [[ "$#" -lt 1 ]]
then
    echo $USAGE
    exit 1
fi
verbose=0
parserr=0
extended=0
# handle optional arguments if they exist, ignore the rest
while [[ $# -gt 0 ]]
    do
    key=$1
    case $key in
        -c | --clean)
            # clean previous output
            CLEAN=1
            shift  # past argument
            ;;
        -e | --parse_errors)
            # include tests that will cause paring errors
            parserr=1
            shift  # past argument
            ;;
        -h | --help)
            # display usage message
            echo $USAGE
            exit 0  # exit here
            ;;
        -v | --verbose)
            # show png output graphs
            verbose=1
            shift  # past argument
            ;;
        -x | --extended)
            # include tests that will cause paring errors
            extended=1
            shift  # past argument
            ;;
        *)  # what remains is assumed to be the MS
            msfile=$1;
            shift  # past unknown arguments
            ;;
    esac
done
if ((CLEAN))
then
    echo -e "${YELLOW} ---- Clear old output images ---- ${NOCOLOR}"
    make clean
fi

# general function to run shadems commands
function runcmd {
    CMD="shadems $msfile $args"
    echo $CMD

    if $CMD
    then
        echo -e "${GREEN} Success ${NOCOLOR}"
    else
        echo -e "${RED} Failure ${NOCOLOR}"
        if ! ((ALLOW_FAILURE))
        then
            exit 1
        fi
    fi
    echo
}


## base functionality following the README
echo -e "${YELLOW} ---- Base functionality tests from README ---- ${NOCOLOR}"
ALLOW_FAILURE=0
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
# "--xaxis CORRECTED_DATA:real,UV --yaxis CORRECTED_DATA:imag,CORRECTED_DATA:amp --field 0 --corr XX,YY"
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


## Extended functionality testing various options used during development
echo -e "${YELLOW} ---- Additional development tests ---- ${NOCOLOR}"
EXTARGS=(
"--xaxis CHAN --yaxis DATA:phase --ant m010,m054 --png plot-antcheck1-DATA-phase-CHAN.png"
"--xaxis CHAN --yaxis DATA:phase --ant-num 0:1,1,3 --png plot-antcheck2-DATA-phase-CHAN.png"
"--xaxis CHAN --yaxis DATA:phase --ant-num 0:2,3 --png plot-antcheck3-DATA-phase-CHAN.png"
"--xaxis CHAN --yaxis DATA:phase --field 0 --corr XX --ymin -180 --ymax 180 --ant-num 0:2,3 --png plot-antcheck4-DATA-phase-CHAN.png"
"--xaxis CHAN --yaxis DATA:phase --field 0 --corr XX --ant m010,m043,m053,m054 --ymin -180 --ymax 180 --png plot-antcheck5-DATA-phase-CHAN.png"
"--xaxis TIME --yaxis amp -C DATA --corr XX,YY --field 0 --ymin -180 --ymax 180"
"--xaxis TIME,FREQ --yaxis DATA:amp:XX,YY --field 0 --corr XX,YY --cmin 0 --cmax 5 --xmin 0.85e9 --xmax 1.712e9"
"--xaxis TIME,FREQ --yaxis DATA:amp:XX,YY --field 0 --cmin 0,0 --cmax 5,5 --xmin 0.85e9 --xmax 1.712e9"
"--xaxis TIME --yaxis amp -C DATA --corr XX,YY --field 0 --chan 10:21:2"
"--xaxis TIME,TIME --yaxis DATA:amp:XX,DATA:amp:YY --field 0"
"--xaxis FREQ --yaxis DATA:amp --field 0 --corr XX,YY --xmin 0.85e9 --xmax 1.712e9"
"--xaxis FREQ --yaxis DATA:amp --field 0 --corr XX,YY --xmin 0.85e9 --xmax 1.712e9 --png plot-withlimits-testim.png"
# iteration
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY --iter-field"
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY --iter-spw"
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY --iter-scan"
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY --iter-corr"
"--xaxis DATA:real,UV --yaxis DATA:imag,DATA:amp --field 0 --corr XX,YY --iter-baseline"
# set colours
"--xaxis FREQ --yaxis DATA:amp --field 0 --corr XX,YY --colour-by DATA:amp --xmin 0.85e9 --xmax 1.712e9 --png plot-colourbyAMP-testim.png"
)
if [[ $extended == 1 ]]
then
    for args in "${EXTARGS[@]}"
    do
        runcmd $msfile $args
    done
fi


## Functional basics following CASA style plotting from MeerKAT cookbook
# # shadems msfiles/spectral_line.ms --xaxis CHAN --yaxis DATA:phase --field 0 --corr XX --ymin -180 --ymax 180 --ant-num 0:3,8
# # shadems msfiles/spectral_line.ms --xaxis CHAN --yaxis DATA:phase --field 0 --corr XX --ant m003,m005,m017,m018,m020 --ymin -180 --ymax 180
# # shadems msfiles/spectral_line.ms --xaxis CHAN --yaxis DATA:phase --field 0 --corr XX --ymin -180 --ymax 180


## induce parser errors
echo -e "${YELLOW} ---- Validate parser will fail as expected ---- ${NOCOLOR}"
ERRARGS=(
# parser error to check len(xaxes) vs len(yaxes)
"--xaxis TIME --yaxis DATA:amp:XX,DATA:amp:YY"
# parser error to check all list options are = len to len(xaxes)
"--xaxis TIME,FREQ --yaxis DATA:amp:XX,YY --cmin 0,0,1 --cmax 5,5,5 --xmin 0.85e9 --xmax 1.712e9"
# parser error to check that both min and max limits are specified
"--xaxis FREQ --yaxis DATA:amp --xmin 0.85e9"
"--xaxis CHAN --yaxis DATA:phase --ymax 180"
# parser error to check channel slicing input
"--xaxis TIME --yaxis amp --chan 10:21,4"
# parser error to check antenna slicing input
# "--xaxis CHAN --yaxis DATA:phase --ant-num 0:1,3 --ant m010,m054"
# "--xaxis TIME --yaxis amp -C DATA --corr XX,YY --field 0 --ant 1:"
)
if [[ $parserr == 1 ]]
then
    ALLOW_FAILURE=1
    for args in "${ERRARGS[@]}"
    do
        runcmd $msfile $args
    done
fi
ALLOW_FAILURE=0

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

