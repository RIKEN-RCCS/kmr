class MapReduce(object):
    def reply_to_spawner(self):
        pass

    def get_spawner_communicator(self, index):
        pass

    def send_kvs_to_spawner(self, kvs):
        pass

    def concatenate_kvs(self, kvss):
        pass

    def map_once(self, mapfn, kvo_key_type=None,
                 rank_zero_only=False, nothreading=False,
                 inspect=False, keep_open=False, take_ckpt=False):
        # HIGH PRIORITY
        pass

    def map_on_rank_zero(self, mapfn, kvo_key_type=None,
                         nothreading=False, inspect=False, keep_open=False,
                         take_ckpt=False):
        # HIGH PRIORITY
        pass

    def read_files_reassemble(self, filename, color, offset, bytes):
        pass

    def read_file_by_segments(self, filename, color):
        pass


class KVS(object):
    def map(self, mapfn, kvo_key_type=None,
            nothreading=False, inspect=False, keep_open=False, take_ckpt=False):
        # HIGH PRIORITY
        pass

    def map_rank_by_rank(self, mapfn, opt):
        pass

    def map_ms(self, mapfn, opt):
        pass

    def map_ms_commands(self, mapfn, opt, sopt):
        pass

    def map_for_some(self, mapfn, opt):
        pass

    def map_via_spawn(self, mapfn, sopt):
        pass

    def map_processes(self, mapfn, nonmpi, sopt):
        pass

    def map_parallel_processes(self, mapfn, sopt):
        pass

    def map_serial_processes(self, mapfn, sopt):
        pass

    def reverse(self, kvo_key_type=None,
                nothreading=False, inspect=False, keep_open=False,
                take_ckpt=False):
        # HIGH PRIORITY (as used in wordcount.py)
        pass

    def reduce(self, redfn, kvo_key_type=None,
               nothreading=False, inspect=False, take_ckpt=False):
        # HIGH PRIORITY
        pass

    def reduce_as_one(self, redfn, opt):
        pass

    def reduce_for_some(self, redfn, opt):
        pass

    def shuffle(self, kvo_key_type=None,
                key_as_rank=False, take_ckpt=False):
        # HIGH PRIORITY
        pass

    def replicate(self, kvo_key_type=None,
                  inspect=False, rank_zero=False, take_ckpt=False):
        # HIGH PRIORITY
        pass

    def distribute(self, cyclic, opt):
        pass

    def sort_locally(self, shuffling, opt):
        pass

    def sort(self, inspect=False):
        # HIGH PRIORITY (as used in wordcount.py)
        pass

    def sort_by_one(self, opt):
        pass

    def free(self):
        # HIGH PRIORITY
        pass

    def add_kv(self, kv_tuple):
        # HIGH PRIORITY
        pass

    def add_kv_done(self):
        # HIGH PRIORITY
        pass

    def get_element_count(self):
        # HIGH PRIORITY
        pass

    def local_element_count(self):
        pass

    def to_list(self):
        # retrieve_kvs_entries
        pass

#    def from_list():
#        pass

    def dump(self, flag):
        pass

    def __str__(self):
        self.dump(0)

    def dump_stats(self, level):
        pass
