#!/usr/bin/env python
# testpyspawn.py

### Spawning Mappers Test

## TESTED: map_via_spawn(), map_processes()

import time
import sys
import os

n_spawns = 4

## SPAWNER SIDE

def run_map_via_spawn(replymode, kmr0):
    ## replymode is one of {"returnkvs", "noreply", "replyeach",
    ## "replyroot"}, which is used also as a subcommand argument.

    NPROCS = kmr0.nprocs
    RANK = kmr0.rank

    if (RANK == 0): print ("RUN map_via_spawn(%s)..." % replymode)

    if (RANK == 0):
        print ("Spawn 2-rank work 4 times using %d dynamic processes."
               % n_spawns)

    k00 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        for _ in range(0, 4):
            k00.add("key", ("maxprocs=2 ./testpyspawn.py ack %s"
                            % replymode))
    k00.add_kv_done()

    def empty_map_fn_mpi(kv, kvi, kvo, index):
        comm = kmr0.get_spawner_communicator(index)
        print ("mapfn[index=%d]: sleeping 7 sec (comm=%d)..." % (index, comm))
        time.sleep(7)
        return 0

    if (replymode == "noreply"):
        mapfn = empty_map_fn_mpi
        options = {}
    elif (replymode == "replyeach"):
        mapfn = empty_map_fn_mpi
        options = {"reply_each" : 1}
    elif (replymode == "replyroot"):
        mapfn = empty_map_fn_mpi
        options = {"reply_root" : 1}
    elif (replymode == "returnkvs"):
        mapfn = kmr4py.kmrso.kmr_receive_kvs_from_spawned_fn
        options = {"reply_each" : 1}
    else:
        raise Exception("BAD: replymode.")

    k01 = k00.map_via_spawn(mapfn, separator_space=1, **options)

    if ((RANK == 0) and (replymode == "returnkvs")):
        assert (k01.local_element_count() == 32)
    else:
        assert (k01.local_element_count() == 0)
    k01.free()
    return

def run_map_processes_seq(kmr0):
    NPROCS = kmr0.nprocs
    RANK = kmr0.rank

    if (RANK == 0): print "RUN map_processes(seq)..."

    if (RANK == 0):
        print "Spawn 2 serial processes 4 times."

    k00 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        for _ in range(0, 4):
            k00.add("key", "maxprocs=2 ./testpyspawn.py seq ignore")
    k00.add_kv_done()

    def empty_map_fn_seq(kv, kvi, kvo, index):
        comm = kmr0.get_spawner_communicator(index)
        print ("mapfn[index=%d]: sleeping 7 sec (comm=%d)..." % (index, comm))
        time.sleep(7)
        return 0

    k01 = k00.map_processes(True, empty_map_fn_seq, separator_space=1)
    k01.free()
    return

def run_map_processes_mpi(kmr0):
    if (RANK == 0): print "RUN map_processes(mpi:noreply)..."

    if (RANK == 0):
        print ("Spawn 2-rank work 4 times using %d dynamic processes.\n"
               % n_spawns)
        print ("** ON SOME IMPLEMENTATIONS OF MPI,"
               + " THIS TEST MAY BLOCK INDEFINITELY. **")
        print ("** THEN, RUN THIS TEST WITH a.out 0"
               + " TO SKIP THIS PART. **")

    k00 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        for _ in range(0, 4):
            k00.add("key", "maxprocs=2 ./testpyspawn.py mpi ignore")
    k00.add_kv_done()

    def empty_map_fn_mpi(kv, kvi, kvo, index):
        comm = kmr0.get_spawner_communicator(index)
        print ("mapfn[index=%d]: sleeping 7 sec (comm=%d)..." % (index, comm))
        time.sleep(7)
        return 0

    k01 = k00.map_processes(False, empty_map_fn_mpi, separator_space=1)
    k01.free()
    return

## SPAWNED SIDE

def spawned_ack(replymode):
    ## Runs a spawned side, started by run_map_via_spawn.

    assert ((replymode == "noreply")
            or (replymode == "replyeach")
            or (replymode == "replyroot")
            or (replymode == "returnkvs"))

    from mpi4py import MPI
    import kmr4py

    dummykmr = kmr4py.KMR("dummy")
    NPROCS = dummykmr.nprocs
    RANK = dummykmr.rank

    print ("Starting spawned-process by map_via_spawn(%s)." % replymode)

    time.sleep(3)

    if (replymode == "noreply"):
        pass
    elif (replymode == "replyeach"):
        dummykmr.reply_to_spawner()
    elif (replymode == "replyroot"):
        if (RANK == 0):
            dummykmr.reply_to_spawner()
    elif (replymode == "returnkvs"):
        dummykmr.reply_to_spawner()
        k00 = dummykmr.make_kvs()
        for i in range (0, 4):
            k00.add(("k" + str(i)), "v" + str(i))
        k00.add_kv_done()
        dummykmr.send_kvs_to_spawner(k00)
    else:
        raise Exception("Bad argument (%s)." % replymode)
    return

def spawned_seq():
    print "spawned_seq."
    return

if (os.path.basename(sys.argv[0]) != "testpyspawn.py"):
    print "command name is not testpyspawn.py."

if (len(sys.argv) == 1):
    ## SPAWNER SIDE

    from mpi4py import MPI
    import kmr4py

    kmr0 = kmr4py.KMR("world")
    kmr0.set_option("single_thread", "1")
    kmr0.set_option("trace_map_spawn", "1")
    kmr0.set_option("spawn_max_processes", "4")

    NPROCS = kmr0.nprocs
    RANK = kmr0.rank

    if (RANK == 0): print "Check spawning mapper"
    if (RANK == 0):
        print ("Running this test needs 4 or more dynamic processes.")

    run_map_via_spawn("noreply", kmr0)
    run_map_via_spawn("replyeach", kmr0)
    run_map_via_spawn("replyroot", kmr0)
    run_map_via_spawn("returnkvs", kmr0)

    #run_map_processes_seq(kmr0)

    ## DO NOT RUN run_map_processes_mpi(), becuase it may hang.

    #run_map_processes_mpi(kmr0)

    kmr0.dismiss()

    if (RANK == 0):
        print ("testpyspawn OK")

elif (len(sys.argv) == 3):
    ## SPAWNED SIDE

    if (sys.argv[1] == "ack"):
        spawned_ack(sys.argv[2])
    elif (sys.argv[1] == "mpi"):
        spawned_seq()
    elif (sys.argv[1] == "seq"):
        spawned_seq()
    else:
        raise Exception("Bad keyword, not mpi/seq (%s)." % sys.argv[1])

else:
    raise Exception("Bad number of arguments.")
