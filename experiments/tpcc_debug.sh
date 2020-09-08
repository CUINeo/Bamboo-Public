cp -r config-std.h config.h

## algorithm
alg=$1
latch=LH_MCSLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
cs_pf="true"
opt_raw="true"
max_waiter=0
# ic3
ic3_eager="false"
ic3_rd="false"

## workload
wl="TPCC"
wh=1
perc=0.5 # payment percentage
user_abort="true"
com="false"

## other
threads=16
profile="true"
cnt=100000
penalty=50000 

threads=32
wh=8

python test_debug.py CC_ALG=$alg LATCH=$latch WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} IC3_EAGER_EXEC=${ic3_eager} IC3_RENDEZVOUS=${ic3_rd} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc TPCC_USER_ABORT=${user_abort} COMMUTATIVE_OPS=$com THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
#
