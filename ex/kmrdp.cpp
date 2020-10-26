/* kmrdp.cpp (2014-02-04) */
/* Copyright (C) 2012-2018 RIKEN R-CCS (only for modifications) */
/* Copyright (C) 2011-2012 AKIYAMA Lab., Tokyo Institute of Technology */

//============================================================================
//
//  Software Name : MPIDP
//
//  Contact address : Tokyo Institute of Technology, AKIYAMA Lab.
//
//============================================================================

/* This is a rewrite of "mpidp.cpp", the original banner is above. */

/** \file kmrdp.cpp MPI-DP Implementation with KMR.  MPI-DP is a tool
    developed by Akiyama Lab., Tokyo Institute of Technology (titec),
    which provides an environment for running almost independent
    (data-intensive, genome search) tasks using MPI, with master-worker
    job scheduling.  This is a rewrite of MPI-DP version 1.0.3.  MEMO:
    RETRY is ignored.  \htmlinclude kmrdp-help.html */

#define MPIDP_VERSION "1.0.3"
#define MPIDP_LASTUPDATED "2011/12/12"

#include <mpi.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <time.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <assert.h>
#include "kmr.h"

using namespace std;

/* Specifies new interface of KMR-DP. */

#define KMRDP 1

/** Asserts and aborts, but it cannot be disabled.  The two message
    styles are for Linux and Solaris.  (The C++ standard does not
    define the macro "__func__", and avoid it, although most compiler
    does extend it).  \hideinitializer */

#ifndef __SVR4
#define xassert(X) \
    ((X) ? (void)(0) \
     : (fprintf(stderr, \
		"%s:%d: Assertion '%s' failed.\n", \
		__FILE__, __LINE__, #X), \
	(void)MPI_Abort(MPI_COMM_WORLD, 1)))
#else
#define xassert(X) \
    ((X) ? (void)(0) \
     : (fprintf(stderr, \
		"Assertion failed: %s, file %s, line %d\n", \
		#X, __FILE__, __LINE__), \
	(void)MPI_Abort(MPI_COMM_WORLD, 1)))
#endif

/** Application Code Entry.  It is necessary to rename "main" to
    "application" in the application.  NOTE IT IS OF C-LINKAGE (that
    is, extern "C").  The main exists in MPI-DP, which sets up MPI,
    reads the configuration table, and then calls the entry of the
    application. */

#if !defined(KMRDP) && (KMRDP == 0)
extern "C" int application(int argc, char *argv[]);
#else
extern "C" int application(int argc, char *argv[]);
extern int (*application_init)(int argc, char *argv[]);
extern int (*application_fin)(int argc, char *argv[]);
#endif

/** Task Log (for MPIDP).  It consists of job name TASKNO, TRY_COUNT,
    control flag STATUS, list of work IDs WORKER, ? RCODE (with 0th
    END, 1st RET, and 2nd FILE). */

typedef struct {
    int taskno;
    int rank;
    int result;
    int fileok;
} TaskRecord;

/** Per-Task Log (for MPIDP). */

typedef struct {
    vector<TaskRecord> records;
    int try_count;
} TaskLog;

/** Per-Rank Worker Log (for MPIDP). */

typedef struct {
    vector<TaskRecord> records;
    int failure;
} RankLog;

/** A Tool to Run Tasks under MPI.  MPI-DP runs tasks which are almost
    independent with master-worker scheduling.  It reads a "jobs-list"
    table, starts MPI processes, and then calls an application entry
    point.  _ARGC and _ARGV are copies of ones passed to main.
    _APPARGV is the unhandled part of argv which is passed to the
    application as argv.  _TABLE_NUMOF_FIELDS on rank0 holds the
    number of fields in the jobs-list table. */

class MPIDP {
  public:
    KMR *_mr;

    /* Rank0 fields: */

    int _argc;
    char **_argv;
    vector<string> _appargv;

    int _out_file_position;
    int _ntry_limit;
    int _worker_life;

    string _title;
    string _jobs_list_file;
    string _parameters;
    vector<string> _table_list;
    int _table_numof_fields;

    char *_host_names;
    TaskLog *_task_logs;
    RankLog *_rank_logs;

    ofstream _logging;

 private:
    /* Disallow copying: */
    MPIDP(MPIDP &dp) {}
    const MPIDP & operator=(const MPIDP &dp);

  public:
    MPIDP() {
	_host_names = 0;
	_task_logs = 0;
	_rank_logs = 0;
    }

    virtual ~MPIDP() {
	if (_host_names != 0) {
	    delete _host_names;
	    _host_names = 0;
	}
	if (_task_logs != 0) {
	    delete [] _task_logs;
	    _task_logs = 0;
	}
	if (_rank_logs != 0) {
	    delete [] _rank_logs;
	    _rank_logs = 0;
	}
    }

    virtual void check_command_line();
    virtual void read_jobs_list();
    virtual void put_task_list(KMR_KVS *kvs);
    virtual vector<string> make_argv_for_task(int index, int retry);
    virtual void put_conf(KMR_KVS *confkvs);
    virtual void copy_conf(KMR_KVS *confkvs);
    virtual void start_task(struct kmr_kv_box kv, const KMR_KVS *kvs,
			    KMR_KVS *kvo);
    virtual void collect_results(const struct kmr_kv_box kv[], const long n,
				 const KMR_KVS *kvs, KMR_KVS *kvo);
    virtual void write_report(ofstream &logging);
};

/* String filled for checking initialized parameter list. */

static const char *dummy_string = "MPIDP";

string erase_spaces(const string &s0, const size_t len);
string replace_pattern(const string &s, const string &key,
		       const string &value);

extern "C" int gather_names(const struct kmr_kv_box kv[], const long n,
			    const KMR_KVS *kvs, KMR_KVS *kvo, void *p);
extern "C" int setup_rank0(const struct kmr_kv_box kv,
			   const KMR_KVS *kvs, KMR_KVS *kvo, void *p,
			   const long i);
extern "C" int list_tasks_rank0(const struct kmr_kv_box kv,
				const KMR_KVS *kvs, KMR_KVS *kvo, void *p,
				const long i);
extern "C" int start_task(struct kmr_kv_box kv, const KMR_KVS *kvs,
			  KMR_KVS *kvo, void *dp_, const long i);
extern "C" int collect_results(const struct kmr_kv_box kv[], const long n,
			       const KMR_KVS *kvs, KMR_KVS *kvo, void *dp_);
extern "C" int write_report(const struct kmr_kv_box kv,
			    const KMR_KVS *kvs, KMR_KVS *kvo, void *p,
			    const long i);

/** A pointer to an MPIDP object for debugging. */

MPIDP *kmr_dp = 0;

/** A pointer to an embedded KMR context for debugging. */

extern "C" KMR *kmr_dp_mr;
KMR *kmr_dp_mr = 0;

/** Initializes MPI and then starts the application. */

int main(int argc, char *argv[])
{
    int cc;
    MPIDP mpidp;
    MPIDP *dp = &mpidp;
    double stime, etime;
    int len;
    char name[MPI_MAX_PROCESSOR_NAME];

    kmr_dp = dp;

    /* Start MPI. */

    int thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    if (thlv == MPI_THREAD_SINGLE) {
	cerr << "[Warning] MPI thread support is single" << endl;
    }
    int nprocs, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    stime = MPI_Wtime();

    dp->_argc = argc;
    dp->_argv = argv;

    cc = kmr_init();
    assert(cc == MPI_SUCCESS);
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    dp->_mr = mr;
    kmr_dp_mr = mr;

    MPI_Get_processor_name(name, &len);
    KMR_KVS *namekvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_kv_box kv;
    kv.klen = (int)sizeof(long);
    kv.k.i = 0;
    kv.vlen = (int)sizeof(MPI_MAX_PROCESSOR_NAME);
    kv.v.p = name;
    cc = kmr_add_kv(namekvs0, kv);
    assert(cc == MPI_SUCCESS);
    cc = kmr_add_kv_done(namekvs0);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *namekvs = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_option opt0 = kmr_noopt;
    opt0.key_as_rank = 1;
    cc = kmr_shuffle(namekvs0, namekvs, opt0);
    assert(cc == MPI_SUCCESS);
    cc = kmr_reduce(namekvs, 0, dp, kmr_noopt, gather_names);
    assert(cc == MPI_SUCCESS);

    /* Read configuration on the master process then copy. */

    KMR_KVS *confkvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map_on_rank_zero(confkvs0, dp, kmr_noopt, setup_rank0);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *confkvs = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_replicate(confkvs0, confkvs, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    dp->copy_conf(confkvs);
    cc = kmr_free_kvs(confkvs);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *taskskvs = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    cc = kmr_map_on_rank_zero(taskskvs, dp, kmr_noopt, list_tasks_rank0);
    assert(cc == MPI_SUCCESS);

    /*if (rank == 0) {printf("setup done\n"); fflush(stdout);}*/

    MPI_Barrier(MPI_COMM_WORLD);
    if (application_init != 0) {
	double t0 = MPI_Wtime();
	(*application_init)(argc, argv);
	MPI_Barrier(MPI_COMM_WORLD);
	double t1 = MPI_Wtime();
	if (mr->trace_kmrdp && rank == 0) {
	    fprintf(stderr, ";;KMR kmrdp:init() (%f msec)\n",
		    ((t1 - t0) * 1e3));
	    fflush(0);
	}
    }

    double tbody0 = MPI_Wtime();

    KMR_KVS *resultskvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    do {
	struct kmr_option opt = kmr_noopt;
	opt.nothreading = 1;
	cc = kmr_map_ms(taskskvs, resultskvs0, dp, opt, start_task);
    } while (cc == MPI_ERR_ROOT);

    MPI_Barrier(MPI_COMM_WORLD);
    double tbody1 = MPI_Wtime();

    if (mr->trace_kmrdp && rank == 0) {
	fprintf(stderr, ";;KMR kmrdp:application() (%f msec)\n",
		((tbody1 - tbody0) * 1e3));
	fflush(stdout);
    }

    if (application_fin != 0) {
	double t0 = MPI_Wtime();
	(*application_fin)(argc, argv);
	MPI_Barrier(MPI_COMM_WORLD);
	double t1 = MPI_Wtime();
	if (mr->trace_kmrdp && rank == 0) {
	    fprintf(stderr, ";;KMR kmrdp:fin() (%f msec)\n",
		    ((t1 - t0) * 1e3));
	    fflush(stdout);
	}
    }

    /*if (rank == 0) {printf("map done\n"); fflush(stdout);}*/

    MPI_Barrier(MPI_COMM_WORLD);
    /*if (rank == 0) {printf("kv_stats:\n");}*/
    /*kmr_dump_kvs_stats(resultskvs0, 1);*/
    /*fflush(stdout);*/

    KMR_KVS *resultskvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_option opt1 = kmr_noopt;
    opt1.key_as_rank = 1;
    cc = kmr_shuffle(resultskvs0, resultskvs1, opt1);
    assert(cc == MPI_SUCCESS);
    cc = kmr_reduce(resultskvs1, 0, dp, kmr_noopt, collect_results);
    assert(cc == MPI_SUCCESS);

    /*if (rank == 0) {printf("reduce done\n"); fflush(stdout);}*/

    KMR_KVS *nonekvs = kmr_create_kvs(mr, KMR_KV_BAD, KMR_KV_BAD);
    cc = kmr_map_on_rank_zero(nonekvs, dp, kmr_noopt, write_report);
    assert(cc == MPI_SUCCESS);
    cc = kmr_free_kvs(nonekvs);
    assert(cc == MPI_SUCCESS);

    etime = MPI_Wtime();

    if (rank == 0) {
	dp->_logging << "\nElapsed time  = "
		     << etime - stime << " sec." << endl;
    }

    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);
    dp->_mr = 0;

    MPI_Finalize();

    return 0;
}

/** Parses an integer string.  It is a safe "atoi". */

static int safe_atoi(const char *s) {
    char gomi;
    int v, cc;
    xassert(s != 0 && s[0] != 0);
    cc = sscanf(s, "%d%c", &v, &gomi);
    xassert(cc == 1);
    return v;
}

/** Replaces the KEY by the VALUE in the source string S. */

string replace_pattern(const string &s, const string &key,
		       const string &value)
{
    string r;
    size_t pos = 0;
    size_t lastpos = 0;
    while ((pos = s.find(key, pos)) != std::string::npos) {
	r.append(s, lastpos, (pos - lastpos));
	r.append(value);
	pos += key.length();
	lastpos = pos;
    }
    r.append(s, lastpos, (s.length() - lastpos));
    return r;
}

/** Deletes white-spaces in the string appearing within LEN characters
    from the start (counting after removing spaces). */

string erase_spaces(const string &s0, const size_t len)
{
    string s = s0;
    size_t i = 0;
    while (i < s.length() && i < len) {
	if (s[i] == ' ') {
	    s.erase(i, 1);
	} else {
	    i++;
	}
    }
    return s;
}

/** Collects host names on all nodes.  It is a reduce-function.  It in
    effect runs only on rank#0. */

extern "C"
int gather_names(const struct kmr_kv_box kv[], const long n,
		 const KMR_KVS *kvs, KMR_KVS *kvo, void *dp_)
{
    /*MPIDP *dp = reinterpret_cast<MPIDP*>(dp_);*/
    MPIDP *dp = (MPIDP *)dp_;
    int nprocs = kvs->c.mr->nprocs;
    int rank = kvs->c.mr->rank;
    xassert(n == nprocs && rank == 0);
    dp->_host_names = new char [nprocs * MPI_MAX_PROCESSOR_NAME];
    for (int i = 0; i < n; i++) {
	strncpy(&(dp->_host_names[i * MPI_MAX_PROCESSOR_NAME]),
		(char *)kv[i].v.p, MPI_MAX_PROCESSOR_NAME);
    }
    return MPI_SUCCESS;
}

/** Reads the command-line options and the jobs-list table, and then
    puts the parameters into the KVS.  It is a map-function, and runs
    only on rank#0. */

extern "C"
int setup_rank0(const struct kmr_kv_box kv,
		const KMR_KVS *kvs, KMR_KVS *kvo, void *dp_, const long i)
{
    /*MPIDP *dp = reinterpret_cast<MPIDP*>(dp_);*/
    MPIDP *dp = (MPIDP *)dp_;
    xassert(kvs == 0 && kv.klen == 0 && kv.vlen == 0);
    dp->check_command_line();
    dp->read_jobs_list();
    dp->put_conf(kvo);
    return MPI_SUCCESS;
}

/** Checks the command-line options. */

void MPIDP::check_command_line()
{
    /* Give initial values to variables; overwritten later. */

    _title = "MPIDP ";
    _title += MPIDP_VERSION;
    _parameters = dummy_string;
    _out_file_position = 0;
    _ntry_limit = 10;
    _worker_life = 3;

    /* Open log file. */

    string logfile = "./mpidp.log";
    for (int i = 1; i < _argc; i++) {
	if (strncmp(_argv[i], "-lg", 3) == 0) {
	    logfile = _argv[++i];
	}
    }
    _logging.open(logfile.c_str());
    if (!_logging) {
	cerr << "[ERROR] Unable to open log file (" << logfile << ")"
	     << endl;
	MPI_Abort(MPI_COMM_WORLD, 1);
	exit(1);
    }

    _logging << "MPIDP ver. " << MPIDP_VERSION << " (KMR-DP)" << endl;
    _logging << "mpidp@bi.cs.titech.ac.jp last updated: "
	     << MPIDP_LASTUPDATED << endl << endl;
    _logging << "#Ranks = " << _mr->nprocs << endl;

    /* Count #ranks on the same rank#0 host. */

    int nprocess = 1;
    string *hosts = new string [_mr->nprocs];
    hosts[0] = &_host_names[0];
    for (int i = 1; i < _mr->nprocs; i++) {
	hosts[i] = &_host_names[i * MPI_MAX_PROCESSOR_NAME];
	if (hosts[i] == hosts[0]) {
	    nprocess++;
	} else {
	    break;
	}
    }
    delete [] hosts;

    _logging << "#Nodes = " << _mr->nprocs/nprocess
	     << " (#Rank/#Nodes = " << nprocess << ")" << endl;

    _logging << endl << "Used nodes - name(rank) :";
    for (int i = 0; i < _mr->nprocs; i++) {
	if (i % 5 == 0) {
	    _logging << endl;
	}
	_logging << "  " << &_host_names[i * MPI_MAX_PROCESSOR_NAME]
		 << "(" << i << ")";
    }
    _logging << endl << endl;
    _logging.flush();

    /* Check command-line options and save remainings in _appargv. */

    _appargv.push_back(_argv[0]);
    for (int i = 1; i < _argc; i++) {
	if (strncmp(_argv[i], "-tb", 3) == 0) {
	    i++;
	    _jobs_list_file = _argv[i];
	    _logging << "Table file    : -tb " << _jobs_list_file << endl;
	} else if (strncmp(_argv[i], "-ot", 3) == 0) {
	    i++;
	    _out_file_position = safe_atoi(_argv[i]);
	    _logging << "Output option : -ot " << _out_file_position << endl;
	} else if (strncmp(_argv[i], "-rt", 3) == 0) {
	    i++;
	    _ntry_limit = safe_atoi(_argv[i]);
	    _logging << "Retry option  : -rt " << _ntry_limit << endl;
	} else if (strncmp(_argv[i], "-wl", 3) == 0) {
	    i++;
	    _worker_life = safe_atoi(_argv[i]);
	    _logging << "Worker life   : -wl " << _worker_life << endl;
	} else if (strncmp(_argv[i], "-pg", 3) == 0) {
	    i++;
	    _logging << "Program name  : -pg " << _argv[i] << endl;
	} else if (strncmp(_argv[i], "-lg", 3) == 0) {
	    i++;
	    _logging << "Log file      : -lg " << _argv[i] << endl;
	} else {
	    _appargv.push_back(_argv[i]);
	}
    }

    if (_appargv.size() > 1) {
	_logging << "Other options :";
	for (size_t i = 1; i < _appargv.size(); i++) {
	    _logging << " " << _appargv[i];
	}
	_logging << endl;
    }
    _logging << endl;
}

/** Puts the run conditions into the KVS, which will be copied to all
    processes.  It runs only on rank#0.  (There is nothing to be
    copied, currently). */

void MPIDP::put_conf(KMR_KVS *kvs)
{
    xassert(kvs->c.mr->rank == 0);
# if 0
    const char *k;
    const char *v;
    char sbuf[256];

    k = "_out_file_position";
    snprintf(sbuf, 256, "%d", _out_file_position);
    v = sbuf;
    cc = kmr_add_string(kvs, k, v);
    assert(cc == MPI_SUCCESS);
#endif
}

/** Initializes the MPI-DP object by copying run conditions stored as
    the key-value pairs.  Copying on rank#0 is totally redundant, as
    overwriting fields by the same contents.  (There is nothing to be
    copied, currently). */

void MPIDP::copy_conf(KMR_KVS *confkvs)
{
#if 0
    const char *s;
    int cc = kmr_find_string(confkvs, "_out_file_position", &s);
    xassert(cc == MPI_SUCCESS);
    _out_file_position = safe_atoi(s);
#endif
}

/** Opens and reads jobs-list file. */

void MPIDP::read_jobs_list()
{
    if (_jobs_list_file.length() == 0) {
	cerr << "[ERROR] No table file specified" << endl;
	MPI_Abort(MPI_COMM_WORLD, 1);
	exit(1);
    }
    ifstream input(_jobs_list_file.c_str(), ios::in);
    if (!input) {
	cerr << "[ERROR] Unable to open table file (" << _jobs_list_file
	     << ")" << endl;
	MPI_Abort(MPI_COMM_WORLD, 1);
	exit(1);
    }

    for (;;) {
	string table;
	if (!getline(input, table)) {
	    break;
	}
	table = erase_spaces(table, 7);
	if (table.length() == 0) {
	    /* Skip an empty line. */
	} else if (string("%#").find_first_of(table[0]) != string::npos) {
	    /* Skip a comment line. */
	} else if (strncasecmp(table.c_str(), "TITLE=", 6) == 0) {
	    _title = table.substr(6);
	    _logging << "TITLE=" << _title << endl;
	} else if (strncasecmp(table.c_str(), "PARAM=", 6) == 0) {
	    _parameters = table.substr(6);
	    _logging << "PARAM=" << _parameters << endl;
	} else {
	    _table_list.push_back(table);
	    //csize = max(table.length(), csize);
	}
    }
    _logging << endl;

    input.close();

    if (strncmp(_parameters.c_str(), dummy_string, 5) == 0) {
	cerr << "[ERROR] PARAM= field not found in table file" << endl;
	MPI_Abort(MPI_COMM_WORLD, 1);
	exit(1);
    }

    int nfields = 1;
    for (size_t i = 0; i < _table_list[0].length(); i++) {
	if (_table_list[0][i] == '\t') {
	    nfields++;
	}
    }
    _table_numof_fields = nfields;
}

/** Puts tasks in the KVS.  It is a map-function, and runs only on
    rank#0. */

extern "C"
int list_tasks_rank0(const struct kmr_kv_box kv,
		     const KMR_KVS *kvs, KMR_KVS *kvo, void *dp_, const long i)
{
    MPIDP *dp = (MPIDP *)dp_;
    xassert(kvs == 0 && kv.klen == 0 && kv.vlen == 0);
    xassert(kvo->c.key_data == KMR_KV_INTEGER
	    && kvo->c.value_data == KMR_KV_OPAQUE);
    dp->put_task_list(kvo);
    return MPI_SUCCESS;
}

/** Puts task entries of TABLE_LIST into KVS, after substituting
    variables in parameters and packing argv in a single string.  */

void MPIDP::put_task_list(KMR_KVS *kvs)
{
    for (int i = 0; i < (int)_table_list.size(); i++) {
	vector<string> newargv = make_argv_for_task(i, 0);

	size_t sz = 0;
	for (size_t j = 0; j < newargv.size(); j++) {
	    sz += (newargv[j].length() + 1);
	}
	sz += 1;
	char *cargv = new char [sz];
	size_t po = 0;
	for (size_t j = 0; j < newargv.size(); j++) {
	    strncpy(&cargv[po], newargv[j].c_str(), (newargv[j].length() + 1));
	    po += (newargv[j].length() + 1);
	}
	xassert((po + 1) == sz);
	cargv[po] = 0;

	struct kmr_kv_box kv;
	kv.klen = (int)sizeof(long);
	kv.k.i = i;
	kv.vlen = (int)sz;
	kv.v.p = cargv;
	kmr_add_kv(kvs, kv);

	delete cargv;
    }
}

/** Makes argument-list (argv) from a task list entry, by substituting
    variables in parameters.  The first argument is used for the
    output file name (or a string "-" if an output file position is
    not specified), and it should be removed when passing to the
    application.  Non-zero RETRY is meaningless.  IT COPIES THE
    RESULT. */

vector<string> MPIDP::make_argv_for_task(int index, int retry)
{
    xassert(0 <= retry && retry <= 999);

    string fields = _table_list[index];
    char *cfields = new char [fields.length() + 1];
    strncpy(cfields, fields.c_str(), (fields.length() + 1));

    string outputfilename = "-";
    string newparams = _parameters;
    int i = 0;
    for (char *e = strtok(cfields, "\t"); e != 0; e = strtok(NULL, "\t")) {
	xassert(i < _table_numof_fields);
	char key[5];
	snprintf(key, 5, "$%d", (i + 1));
	string k = key;
	string v = e;
	if (_out_file_position == (i + 1)) {
	    if (retry != 0) {
		char tag[5];
		snprintf(tag, 5, ".%d", retry);
		v += tag;
	    }
	    outputfilename = v;
	}
	newparams = replace_pattern(newparams, k, v);
	i++;
    }
    xassert(i == _table_numof_fields);

    char *cparams = new char [newparams.length() + 1];
    strncpy(cparams, newparams.c_str(), (newparams.length() + 1));

    vector<string> newargv;
    newargv.push_back(outputfilename);
    newargv.insert(newargv.end(), _appargv.begin(), _appargv.end());
    for (char *e = strtok(cparams, " "); e != 0; e = strtok(NULL, " ")) {
	newargv.push_back(e);
    }

    delete cfields;
    delete cparams;

    return newargv;
}

/** Starts an application using argv passed in value data.  It is a
    map-function. */

extern "C"
int start_task(struct kmr_kv_box kv, const KMR_KVS *kvs,
	       KMR_KVS *kvo, void *dp_, const long i)
{
    MPIDP *dp = (MPIDP *)dp_;
    dp->start_task(kv, kvs, kvo);
    return MPI_SUCCESS;
}

/* Counts argv list. */

static int count_args(char *args, size_t len)
{
    int wargc = 0;
    size_t po = 0;
    for (;;) {
	xassert(po < len);
	size_t z = strlen(&args[po]);
	if (z == 0) {
	    break;
	}
	wargc++;
	po += (z + 1);
    }
    xassert((po + 1) == len);
    return wargc;
}

/* Scans and fills argv list. */

static void scan_args(char **wargv, int wargc, char *args, size_t len)
{
    size_t po = 0;
    for (int i = 0; i < wargc; i++) {
	xassert(po < len);
	wargv[i] = &args[po];
	size_t z = strlen(&args[po]);
	xassert(z != 0);
	po += (z + 1);
    }
    xassert((po + 1) == len);
}

/** Starts an application using argv in value data.  Note that the
    passed argv has an extra entry indicating the output file name in
    the first, and it is dropped. */

void MPIDP::start_task(struct kmr_kv_box kv, const KMR_KVS *kvs, KMR_KVS *kvo)
{
    int rank = kvs->c.mr->rank;
    xassert(rank != 0);
    xassert(kvs->c.key_data == KMR_KV_INTEGER
	    && kvs->c.value_data == KMR_KV_OPAQUE);

    int taskno = (int)kv.k.i;
    char *cargv = (char *)kv.v.p;

    int wargc = count_args(cargv, (size_t)kv.vlen);
    xassert(wargc > 1);
    char **wargv = new char * [wargc];
    scan_args(wargv, wargc, cargv, (size_t)kv.vlen);

    string outputfilename = wargv[0];
    int xargc = wargc - 1;
    char **xargv = wargv + 1;

    if (0) {
	string fields;
	fields += xargv[0];
	for (int i = 1; i < xargc; i++) {
	    fields += " ";
	    fields += xargv[i];
	}
	printf("Running task=%d on rank=%d param=%s\n",
	       taskno, rank, fields.c_str());
	fflush(stdout);
    }

    /* RUN THE APPLICATION. */

    xassert(taskno < (1 << 20));

    TaskRecord tc;
    tc.taskno = taskno;
    tc.rank = rank;
    struct stat buf;
    int cc;
    try {
	cc = application(xargc, xargv);
	tc.result = cc;
    } catch (char *e) {
	cerr << "[ERROR] application exception : " << e << endl;
	tc.result = -1;
    }

    if (outputfilename != "-") {
	cc = stat(outputfilename.c_str(), &buf);
	tc.fileok = ((cc == 0) ? 1 : -1);
    } else {
	tc.fileok = 0;
    }

    long zero = 0;
    cc = kmr_add_kv1(kvo, &zero, (int)sizeof(long), &tc, (int)sizeof(tc));
    assert(cc == MPI_SUCCESS);

    delete wargv;
}

/** Collects result status in ir[] on rank#0.  It is a
    reduce-function.  It in effect runs only on rank#0. */

extern "C"
int collect_results(const struct kmr_kv_box kv[], const long n,
		    const KMR_KVS *kvs, KMR_KVS *kvo, void *dp_)
{
    MPIDP *dp = (MPIDP *)dp_;
    dp->collect_results(kv, n, kvs, kvo);
    return MPI_SUCCESS;
}

/** Collects result status in ir[] on rank#0. */

void MPIDP::collect_results(const struct kmr_kv_box kv[], const long n,
			    const KMR_KVS *kvs, KMR_KVS *kvo)
{
    xassert(kvo == 0);
    xassert(kvs->c.mr->rank == 0);
    xassert((size_t)n == _table_list.size());

    xassert(_task_logs == 0);
    _task_logs = new TaskLog [n];

    xassert(_rank_logs == 0);
    int nprocs = kvs->c.mr->nprocs;
    _rank_logs = new RankLog [nprocs];

    for (int i = 0; i < n; i++) {
	_task_logs[i].try_count = 0;
    }
    for (int i = 0; i < n; i++) {
	xassert(kv[i].klen == sizeof(long)
		&& kv[i].vlen == sizeof(TaskRecord));
	//long k = kv[i].k.i;
	TaskRecord tc = *((TaskRecord *)kv[i].v.p);
	_task_logs[i].records.push_back(tc);
	_rank_logs[tc.rank].records.push_back(tc);
    }
}

/** Writes jobs and workers report in log.  It is a map-function, and
    runs only on rank#0. */

extern "C"
int write_report(const struct kmr_kv_box kv,
		 const KMR_KVS *kvs, KMR_KVS *kvo, void *dp_, const long i)
{
    xassert(kvs == 0 && kv.klen == 0 && kv.vlen == 0);
    MPIDP *dp = (MPIDP *)dp_;
    dp->write_report(dp->_logging);
    return MPI_SUCCESS;
}

/** Writes jobs and workers report in log. */

void MPIDP::write_report(ofstream &logging)
{
    size_t ntasks = _table_list.size();

    /* Write result for tasks. */

    logging << "JOB table :" << endl;
    for (size_t i = 0; i < ntasks; i++) {
	xassert(_task_logs[i].records.size() > 0);
	logging << _task_logs[i].records[0].taskno
		<< " EXEC=" << _task_logs[i].try_count;
	for (size_t j = 0; j < _task_logs[i].records.size(); j++) {
	    TaskRecord tc = _task_logs[i].records[j];
	    logging << " (WID=" << tc.taskno;
	    logging << " END=" << 1;
	    logging << " RET=" << tc.result;
	    logging << " FILE=" << tc.fileok << ")";
	}
	logging << endl;
    }

    /* Write result for ranks. */

    int nprocs = _mr->nprocs;
    logging << "\nWorker table :" << endl;
    for (int i = 1; i < nprocs; i++) {
	logging << i << "\t" << _rank_logs[i].records.size();
	for (size_t j = 0; j < _rank_logs[i].records.size(); j++) {
	    TaskRecord tc = _rank_logs[i].records[j];
	    logging << "\t" << tc.taskno << "\t" << tc.result;
	}
	logging << endl;
    }

    return;
}

/*
Copyright (C) 2012-2018 RIKEN R-CCS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/

/* Local Variables: */
/* c-basic-offset: 4 */
/* End: */
