#!/usr/bin/python3

"""
A script to generate the data file as required
by the assignment. The data file generated is
named 'log.txt', and overwrites any existing
file. A command line parameter can be passed
stating the number of records to be generated.
"""

import random
import argparse

_author_ = 'Abhijeet Krishnan'
_enrollment_number_ = 'BT13CSE001'

random.seed(1)

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--records", type=int, help="number of records", default=10, metavar="NUM_RECORDS")
parser.add_argument("--luid", type=int, help="left range of UserId",   default=100000)
parser.add_argument("--ruid", type=int, help="right range of UserId",  default=1000000)
parser.add_argument("--ltid", type=int, help="left range of TrackId",  default=100)
parser.add_argument("--rtid", type=int, help="right range of TrackId", default=1000)
args = parser.parse_args()

if args.luid >= args.ruid:
    parser.error("left range of UserId must be less than right range")
elif not 100000 <= args.luid <= 999999:
    parser.error("left range of UserId must be in the range [100000, 999999]")
elif not 100001 <= args.ruid <= 1000000:
    parser.error("right range of userId must be within range [100001, 1000000]")
elif args.ltid >= args.rtid:
    parser.error("left range of TrackId must be less than right range")
elif not 100 <= args.ltid <= 999:
    parser.error("left range of TrackId must be within range [100, 999]")
elif not 101 <= args.rtid <= 1000:
    parser.error("right range of TrackId must be within range [101, 1000]")

fp = open('log.txt', 'w')

fp.write('UserId|TrackId|Shared|Radio|Skip\n') # print header

for i in range(args.records):

    ## Generate UserId
    # UserId is a 6-digit number
    user_id = random.randrange(args.luid, args.ruid)

    ## Generate TrackId
    # TrackId is a 3-digit number
    track_id = random.randrange(args.ltid, args.rtid)

    ## Generate Shared
    # Shared is either '0' or '1'
    shared = random.choice([0, 1])

    ## Generate Radio
    # Radio is either '0' or '1'
    radio = random.choice([0, 1])

    # Generate Skip
    # Skip is either '0' or '1'
    skip = random.choice([0, 1]) if radio == 1 else 0

    fp.write('%d|%d|%d|%d|%d\n' % (user_id, track_id, shared, radio, skip))

fp.close()
