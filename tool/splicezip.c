/* splicezip.c (2021-06-22) */
/* Copyright (C) 2021-2021 RIKEN R-CCS */

/* Concatenating PK-ZIP files. */

/* This simply concatenates PK-ZIP files and appends a new directory
   collected from the original files.  The name "splicezip" avoided
   the words "concatenate", "merge", "append", etc., because these are
   often used with "zip" in functional languages.  It also reserved
   "zipsplice" to an official tool.  A man page of "zip" says,
   surprisingly, it will work on a concatenation of zip files despite
   the file becomes ill-formatted. */

/* See https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT for
   the specification. */

/* (*) It is recomended to check the file after creation with "unzip
   -t" and "zip -F" (CRC checks). */
/* (*) It always creates a zip64 file. */
/* (*) It does not modify local_file records. */
/* (*) It does not remove duplicates of names. */
/* (*) It does not support the "Strong Encryption Specification". */
/* (*) It does not supprt split disks images. */
/* (*) Finding an EOCD record is limited to the last 1MB. */

/* It always creates a zip64 file by appending a directory with an
   EOCD64 and an EOCD locator.  Also, it may replace a 32-bit offset
   to a local_file record (relative_offset_of_local_header) with a
   64-bit one, and adds (or replaces) an extended_information to a
   local_file record.  It only changes the offset to a local_file
   record, and that it can use local_file records unmodified.  Note
   that the checks "zipinfo -v" performs require some coherency
   between a local_file record and a central directory entry. */

/* A condition for ZIP64 used in find_ecrec64 in Info-Zip
   uzip/process.c: "(G.ecrec.number_this_disk=0xFFFF)
   ||(G.ecrec.number_this_disk=ecloc64_total_disks-1)" where zip64
   total-disks starts from 1. */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <math.h>
#include <unistd.h>
#include <inttypes.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/mman.h>
#include <errno.h>
#include <assert.h>

/* Common Acronyms: */
/* EOCD: end of central directory record */
/* ECDL: end of central directory locator */

typedef uint8_t byte;
typedef struct byte2 {byte b[2];} byte2;
typedef struct byte4 {byte b[4];} byte4;
typedef struct byte8 {byte b[8];} byte8;

/* ** Copy of structures from APPNOTE.TXT. ** */

/* 4.3.6 Overall .ZIP file format: */

/*
 * {local_file_record
 *  encryption header
 *  [file_data]
 *  data_descriptor} (x N times)
 * archive decryption header
 * archive extra data record
 * {central_directory} (x N times)
 * zip64_end_of_central_directory (record)
 * zip64_end_of_central_directory_locator
 * end_of_central_directory (record)
 */

/* 4.3.7  Local file header */

/* (This is not used in this program). */

struct local_file_record {
    byte4 signature; /*0x04034b50*/
    byte2 version_needed_to_extract;
    byte2 general_purpose_bit_flag;
    byte2 compression_method;
    byte2 last_mod_file_time;
    byte2 last_mod_file_date;
    byte4 crc32;
    byte4 compressed_size;
    byte4 uncompressed_size;
    byte2 file_name_length;
    byte2 extra_field_length;
    /* file_name[file_name_length] */
    /* extra_field[extra_field_length] */
};

/* (4.3.8  File data) */

/* 4.3.9  Data descriptor */

/* (These are not used in this program). */

struct data_descriptor32 {
    byte4 crc32;
    byte4 compressed_size;
    byte4 uncompressed_size;
};

struct data_descriptor64 {
    byte4 crc32;
    byte8 compressed_size;
    byte8 uncompressed_size;
};

/* (4.3.10  Archive decryption header) */
/* (4.3.11  Archive extra data record) */

/* 4.3.12  Central directory structure */

/* An entry in the central directory. */

struct central_directory {
    byte4 signature; /*0x02014b50*/
    byte2 version_made_by;
    byte2 version_needed_to_extract;
    byte2 general_purpose_bit_flag;
    byte2 compression_method;
    byte2 last_mod_file_time;
    byte2 last_mod_file_date;
    byte4 crc32;
    byte4 compressed_size;
    byte4 uncompressed_size;
    byte2 file_name_length;
    byte2 extra_field_length;
    byte2 file_comment_length;
    byte2 disk_number_start;
    byte2 internal_file_attributes;
    byte4 external_file_attributes;
    byte4 relative_offset_of_local_header;
    /* file_name[file_name_length] */
    /* extra_field[extra_field_length] */
    /* file_comment[file_comment_length] */
};

/* 4.3.13 Digital signature */

struct digital_signature {
    byte4 signature; /*0x05054b50*/
    byte2 size_of_data;
    /*byte signature_data[size_of_data];*/
};

/* 4.3.14  Zip64 end of central directory record */

struct zip64_end_of_central_directory {
    byte4 signature; /*0x06064b50*/
    byte8 size_of_zip64_end_of_central_directory;
    byte2 version_made_by;
    byte2 version_needed_to_extract;
    byte4 number_of_this_disk;
    byte4 number_of_the_disk_with_the_start_of_the_central_directory;
    byte8 total_number_of_entries_in_the_central_directory_on_this_disk;
    byte8 total_number_of_entries_in_the_central_directory;
    byte8 size_of_the_central_directory;
    byte8 offset_of_start_of_central_directory;
    /*byte zip64_extensible_data_sector[];*/
};

/* 4.3.15 Zip64 end of central directory locator */

struct zip64_end_of_central_directory_locator {
    byte4 signature; /*0x07064b50*/
    byte4 number_of_the_disk;
    byte8 relative_offset_of_the_zip64_end_of_central_directory;
    byte4 total_number_of_disks;
};

/* 4.3.16  End of central directory record */

struct end_of_central_directory {
    byte4 signature; /*0x06054b50*/
    byte2 number_of_this_disk;
    byte2 number_of_the_disk_with_the_start_of_the_central_directory;
    byte2 total_number_of_entries_in_the_central_directory_on_this_disk;
    byte2 total_number_of_entries_in_the_central_directory;
    byte4 size_of_the_central_directory;
    byte4 offset_of_start_of_central_directory;
    byte2 zip_file_comment_length;
    /* zip_file_comment[zip_file_comment_length] */
};

/* 4.5.3 -Zip64 Extended Information Extra Field (0x0001) */

/* An extra_field is a variable length record, with the size length+4
   (4 for the header).  Extra_fields are stored in a local_file record
   and a central_directory entry.  An extended_information
   (tag=0x0001) is one kind of an extra_field, which stores values
   that exceeds the sizes of zip32.  Only the slots that do not fit in
   zip32 appear in the record in the specific order:
   uncompressed_size; compressed_size;
   relative_offset_of_local_header; number_of_this_disk.  Note that
   "splicezip" only may add a relative_offset_of_local_header. */

struct extra_field_header {
    byte2 tag;
    byte2 length;
    /* byte data[length]; */
};

/* A fully populated extended_information.  It is used to take the
   maximum size of this record. */

struct maximum_extended_information {
    byte2 tag; /*0x0001*/
    byte2 length;
    byte8 uncompressed_size;
    byte8 compressed_size;
    byte8 relative_offset_of_local_header;
    byte4 number_of_this_disk;
};

/* ** Structures used in this tool. ** */

union eocd32_eocd64 {
    struct end_of_central_directory eocd32;
    struct zip64_end_of_central_directory eocd64;
};

/* An EOCD information.  "ok" indicates an EOCD is found.  "position" is
   the file-pointer of an EOCD.  */

struct eocd_block {
    bool ok;
    bool zip64;
    off_t position;
    off_t cd_start;
    uint64_t cd_size;
    size_t cd_n_entries;
};

/* A central_directory information of each zip file.  It holds a
   central_directory.  The directory_size_full is the size of
   central_directory in the file (with a digital_signature), and the
   directory_size is one without a digital_signature block. */

struct cd_block {
    struct central_directory *directory;
    char *zip_file_name;
    size_t zip_file_size;
    size_t n_entries;
    size_t directory_size;
    size_t directory_size_full;
};

/* A record to hold values of extended_information.  Each slot is
   paired with a flag indicating it appears in an extended_information
   if true. */

struct extended_information {
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint64_t relative_offset_of_local_header;
    uint32_t number_of_this_disk;
    bool usize:1;
    bool csize:1;
    bool lf:1;
    bool diskno:1;
};

/* A marker value when some field is displaced in zip64. */

const uint32_t EJECTED32 = 0xffffffff;

/* An option (for testing) to wipe off the directory in the input zip
   file. */

bool wipe_off_old_directory = false;

/* An option (for testing) to always use an extended_information
   (zip64) for sizes and positions (even when they are within
   32-bits).  "zipinfo -v" generates a warning for this option. */

bool force_use_of_extra_field = false;

/* A verbosity in 0 (-q), 1, 2 (-v), 3, 4. */

int verbosity = 1;

/* A temporary buffer for an eocd record.  It is filled by
   find_eocd. */

union eocd32_eocd64 eocd_record;

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

uint16_t
byte2_value(byte2 x)
{
    return (uint16_t)(((uint16_t)x.b[0] << 0*8)
		      | ((uint16_t)x.b[1] << 1*8));
}

uint32_t
byte4_value(byte4 x)
{
    return (uint32_t)(((uint32_t)x.b[0] << 0*8)
		      | ((uint32_t)x.b[1] << 1*8)
		      | ((uint32_t)x.b[2] << 2*8)
		      | ((uint32_t)x.b[3] << 3*8));
}

uint64_t
byte8_value(byte8 x)
{
    return (uint64_t)(((uint64_t)x.b[0] << 0*8)
		      | ((uint64_t)x.b[1] << 1*8)
		      | ((uint64_t)x.b[2] << 2*8)
		      | ((uint64_t)x.b[3] << 3*8)
		      | ((uint64_t)x.b[4] << 4*8)
		      | ((uint64_t)x.b[5] << 5*8)
		      | ((uint64_t)x.b[6] << 6*8)
		      | ((uint64_t)x.b[7] << 7*8));
}

struct byte2
byte2_bytes(uint16_t v)
{
    struct byte2 vv = {
	.b = {
	    ((v >> (0*8)) & 0xff),
	    ((v >> (1*8)) & 0xff)}
    };
    return vv;
}

struct byte4
byte4_bytes(uint32_t v)
{
    struct byte4 vv = {
	.b = {
	    ((v >> (0*8)) & 0xff),
	    ((v >> (1*8)) & 0xff),
	    ((v >> (2*8)) & 0xff),
	    ((v >> (3*8)) & 0xff)}
    };
    return vv;
}

struct byte8
byte8_bytes(uint64_t v)
{
    struct byte8 vv = {
	.b = {
	    ((v >> (0*8)) & 0xff),
	    ((v >> (1*8)) & 0xff),
	    ((v >> (2*8)) & 0xff),
	    ((v >> (3*8)) & 0xff),
	    ((v >> (4*8)) & 0xff),
	    ((v >> (5*8)) & 0xff),
	    ((v >> (6*8)) & 0xff),
	    ((v >> (7*8)) & 0xff)}
    };
    return vv;
}

/* Calls pread and repeats until fully read.  It retains errno from
   the last pread at a return. */

ssize_t
pread_whole(int fd, void *buf0, size_t nbyte, off_t off)
{
    byte *buf = buf0;
    size_t cnt = 0;
    while (cnt < nbyte) {
	ssize_t ss = pread(fd, (buf + cnt), (nbyte - cnt), (off + cnt));
	if (ss == -1) {
	    if (errno == EINTR || errno == EAGAIN) {
		continue;
	    } else {
		/*
		 * char ee[128];
		 * snprintf(ee, sizeof(ee), "pread failed (%s): %s", msg, m);
		 * fprintf(stderr, "%s.\n", ee);
		 * fflush(0);
		 * exit(1);
		 */
		assert(0);
		return -1;
	    }
	} else if (ss == 0) {
	    return cnt;
	}
	cnt += ss;
    }
    return (ssize_t)cnt;
}

ssize_t
pwrite_whole(int fd, const void *buf0, size_t nbyte, off_t offset)
{
    const byte *buf = buf0;
    size_t cnt = 0;
    while (cnt < nbyte) {
	ssize_t ss = pwrite(fd, (buf + cnt), (nbyte - cnt), (offset + cnt));
	if (ss == -1) {
	    if (errno == EINTR || errno == EAGAIN) {
		continue;
	    } else {
		/*
		 * char ee[128];
		 * snprintf(ee, sizeof(ee), "pread failed (%s): %s", msg, m);
		 * fprintf(stderr, "%s.\n", ee);
		 * fflush(0);
		 * exit(1);
		 */
		assert(0);
		return -1;
	    }
	} else if (ss == 0) {
	    errno = EIO;
	    assert(0);
	    return -1;
	}
	cnt += ss;
    }
    return (ssize_t)cnt;
}

ssize_t
write_whole(int fd, const void *buf0, size_t nbyte)
{
    const byte *buf = buf0;
    size_t cnt = 0;
    while (cnt < nbyte) {
	ssize_t ss = write(fd, (buf + cnt), (nbyte - cnt));
	if (ss == -1) {
	    if (errno == EINTR || errno == EAGAIN) {
		continue;
	    } else {
		/*
		 * char ee[128];
		 * snprintf(ee, sizeof(ee), "pread failed (%s): %s", msg, m);
		 * fprintf(stderr, "%s.\n", ee);
		 * fflush(0);
		 * exit(1);
		 */
		assert(0);
		return -1;
	    }
	} else if (ss == 0) {
	    errno = EIO;
	    assert(0);
	    return -1;
	}
	cnt += ss;
    }
    return (ssize_t)cnt;
}

void
dump_end_of_central_directory32(struct end_of_central_directory *e)
{
    if (verbosity < 2) {
	return;
    }

    printf("** (eocd32)\n");
    printf("number_of_this_disk=%d\n",
	   byte2_value(e->number_of_this_disk));
    printf("number_of_the_disk_with_the_start_of_the_central_directory=%d\n",
	   byte2_value(e->number_of_the_disk_with_the_start_of_the_central_directory));
    printf("total_number_of_entries_in_the_central_directory_on_this_disk=%d\n",
	   byte2_value(e->total_number_of_entries_in_the_central_directory_on_this_disk));
    printf("total_number_of_entries_in_the_central_directory=%d\n",
	   byte2_value(e->total_number_of_entries_in_the_central_directory));
    printf("size_of_the_central_directory=%d\n",
	   byte4_value(e->size_of_the_central_directory));
    printf("offset_of_start_of_central_directory=%u\n",
	   byte4_value(e->offset_of_start_of_central_directory));
    printf("zip_file_comment_length=%d\n",
	   byte2_value(e->zip_file_comment_length));
    fflush(0);
}

void
dump_end_of_central_directory64(struct zip64_end_of_central_directory *e)
{
    if (verbosity < 2) {
	return;
    }

    printf("** (eocd64)\n");
    printf("size_of_zip64_end_of_central_directory=%ld\n",
	   byte8_value(e->size_of_zip64_end_of_central_directory));
    printf("version_made_by=%02x %02x\n",
	   e->version_made_by.b[0],
	   e->version_made_by.b[1]);
    printf("version_needed_to_extract=%02x %02x\n",
	   e->version_needed_to_extract.b[0],
	   e->version_needed_to_extract.b[1]);
    printf("number_of_this_disk=%d\n",
	   byte4_value(e->number_of_this_disk));
    printf("number_of_the_disk_with_the_start_of_the_central_directory=%d\n",
	   byte4_value(e->number_of_the_disk_with_the_start_of_the_central_directory));
    printf("total_number_of_entries_in_the_central_directory_on_this_disk=%ld\n",
	   byte8_value(e->total_number_of_entries_in_the_central_directory_on_this_disk));
    printf("total_number_of_entries_in_the_central_directory=%ld\n",
	   byte8_value(e->total_number_of_entries_in_the_central_directory));
    printf("size_of_the_central_directory=%ld\n",
	   byte8_value(e->size_of_the_central_directory));
    printf("offset_of_start_of_central_directory=%lu\n",
	   byte8_value(e->offset_of_start_of_central_directory));
    fflush(0);
}

/* Checks the headers of the records of central_directory and
   local_file record.  The positions are file-poistions. */

bool
check_headers(int fd, off_t cdpos, off_t lfpos, __attribute__((unused)) char *filename)
{
    /* The central_directory entry marker: */
    const uint32_t de_marker = 0x02014b50;
    /* The local_file_record marker: */
    const uint32_t lf_marker = 0x04034b50;

    byte4 w;

    ssize_t ss0 = pread_whole(fd, &w, sizeof(w), cdpos);
    if (ss0 == -1 || ss0 != sizeof(w)) {
	return false;
    }
    uint32_t m1 = byte4_value(w);
    if (m1 != de_marker) {
	return false;
    }
    ssize_t ss1 = pread_whole(fd, &w, sizeof(w), lfpos);
    if (ss1 == -1 || ss1 != sizeof(w)) {
	return false;
    }
    uint32_t m2 = byte4_value(w);
    if (m2 != lf_marker) {
	return false;
    }
    return true;
}

/* Checks an EOCD record is a true one.  It assumes eocd_record.eocd32
   contains an eocd-record for zip32 at this time.  It uses the same
   checks of "verifyEND" in OpenJDK's "libzip".  */

bool
check_eocd32(struct end_of_central_directory *eocd32,
	     int fd, off_t eocd32pos, char *filename)
{
    /* The end_of_central_directory marker: */
    const uint32_t eocd32_marker = 0x06054b50;

    /*struct end_of_central_directory eocd32 = eocd_record.eocd32;*/
    uint32_t m0 = byte4_value(eocd32->signature);
    assert(m0 == eocd32_marker);

    uint32_t siz = byte4_value(eocd32->size_of_the_central_directory);
    uint32_t off = byte4_value(eocd32->offset_of_start_of_central_directory);
    off_t cdpos = eocd32pos - siz;
    off_t lfpos = eocd32pos - siz - off;
    /*printf("** LFPOS32=%ld\n", lfpos);*/
    if (!(cdpos >= 0 && lfpos >= 0)) {
	return false;
    }
    bool ok = check_headers(fd, cdpos, lfpos, filename);
    return ok;
}

/* Checks a possible EOCD64 record is a true one.  It fills
   eocd_record.eocd64, if it is zip64. */

bool
check_eocd64(struct zip64_end_of_central_directory *eocd64,
	     int fd, off_t eocd64pos, __attribute__((unused)) char *filename)
{
    /* The zip64_end_of_central_directory marker: */
    const uint32_t eocd64_marker = 0x06064b50;

    /*struct zip64_end_of_central_directory eocd64 = eocd_record.eocd64;*/
    uint32_t m = (uint32_t)byte4_value(eocd64->signature);
    if (m != eocd64_marker) {
	return false;
    }

    uint64_t siz = byte8_value(eocd64->size_of_the_central_directory);
    uint64_t off = byte8_value(eocd64->offset_of_start_of_central_directory);
    off_t cdpos = eocd64pos - siz;
    off_t lfpos = eocd64pos - siz - off;
    /*printf("** LFPOS64=%ld\n", lfpos);*/
    if (!(cdpos >= 0 && lfpos >= 0)) {
	return false;
    }
    bool ok = check_headers(fd, cdpos, lfpos, filename);
    return ok;
}

/* Finds an EOCD record near the end of a file.  It reads the file at
   the tail, and scans the EOCD marker.  It also fills eocd_record. */

struct eocd_block
find_eocd(int fd, char *filename)
{
    const size_t check_size = 1024 * 1024;
    struct stat st;
    const uint32_t eocd32_marker = 0x06054b50;
    const uint32_t ecdl_marker = 0x07064b50;
    const byte marker0 = (eocd32_marker & 0xff);
    size_t ecdl_size = sizeof(struct zip64_end_of_central_directory_locator);
    byte b[check_size];

    int cc = fstat(fd, &st);
    if (cc == -1) {
	__attribute__((unused)) int x = errno;
	perror("fstat failed (for finding EOCD)");
	exit(1);
    }
    if (!S_ISREG(st.st_mode)) {
	fprintf(stderr, "a file is not a regular file.\n");
	exit(1);
    }

    off_t offset = MAX(st.st_size - (off_t)check_size, 0);
    ssize_t ss = pread_whole(fd, b, check_size, offset);
    if (ss == -1) {
	__attribute__((unused)) int x = errno;
	perror("pread failed (for finding EOCD)");
	assert(0);
    }

    off_t sz = ss;
    size_t eocd_size = sizeof(struct end_of_central_directory);
    assert((size_t)sz >= eocd_size && (size_t)sz <= check_size);
    off_t eocd_position = -1;
    bool zip64 = false;
    for (byte *q = &b[sz] - eocd_size; q >= b; q--) {
	if (*q == marker0) {
	    byte4 *mx = (void *)q;
	    uint32_t m0 = byte4_value(*mx);
	    if (m0 == eocd32_marker) {

		/* Check ZIP32, first. */

		struct end_of_central_directory *eocd32 = (void *)q;
		off_t eocd32pos = (offset + (q - b));
		bool ok32 = check_eocd32(eocd32, fd, eocd32pos, filename);

		/* Check ZIP64. */

		assert((size_t)(q - b) >= ecdl_size);
		struct zip64_end_of_central_directory_locator *ecdl = (void *)(q - ecdl_size);
		uint32_t m1 = (uint32_t)byte4_value(ecdl->signature);
		if (m1 == ecdl_marker) {
		    off_t eocd64pos = byte8_value(ecdl->relative_offset_of_the_zip64_end_of_central_directory);
		    off_t off = (eocd64pos - offset);
		    assert(off >= 0 && (size_t)off <= check_size);
		    struct zip64_end_of_central_directory *eocd64 = (void *)&b[off];
		    bool ok64 = check_eocd64(eocd64, fd, eocd64pos, filename);
		    if (ok64) {
			dump_end_of_central_directory32(eocd32);
			dump_end_of_central_directory64(eocd64);

			eocd_record.eocd64 = *eocd64;
			eocd_position = eocd64pos;
			zip64 = true;
			break;
		    }
		}
		if (ok32) {
		    dump_end_of_central_directory32(eocd32);

		    eocd_record.eocd32 = *eocd32;
		    eocd_position = eocd32pos;
		    zip64 = false;
		    break;
		}
	    }
	}
    }
    if (eocd_position == -1) {
	fprintf(stderr, "a file does not contain an EOCD.\n");
	exit(1);
    }

    if (0) {
	if (zip64 == false) {
	    struct end_of_central_directory *e = &eocd_record.eocd32;
	    dump_end_of_central_directory32(e);
	} else {
	    struct zip64_end_of_central_directory *e = &eocd_record.eocd64;
	    dump_end_of_central_directory64(e);
	}
    }

    if (zip64 == false) {
	struct end_of_central_directory eocd32 = eocd_record.eocd32;
	assert(byte2_value(eocd32.number_of_this_disk) == 0);
	assert(byte2_value(eocd32.number_of_the_disk_with_the_start_of_the_central_directory) == 0);
	assert(byte2_value(eocd32.total_number_of_entries_in_the_central_directory)
	       == byte2_value(eocd32.total_number_of_entries_in_the_central_directory_on_this_disk));
    } else {
	struct zip64_end_of_central_directory eocd64 = eocd_record.eocd64;
	assert(byte4_value(eocd64.number_of_this_disk) == 0);
	assert(byte4_value(eocd64.number_of_the_disk_with_the_start_of_the_central_directory) == 0);
	assert(byte8_value(eocd64.total_number_of_entries_in_the_central_directory)
	       == byte8_value(eocd64.total_number_of_entries_in_the_central_directory_on_this_disk));
    }

    uint64_t cdsiz;
    off_t cdpos;
    size_t nent;
    if (zip64 == false) {
	struct end_of_central_directory eocd32 = eocd_record.eocd32;
	cdsiz = byte4_value(eocd32.size_of_the_central_directory);
	cdpos = byte4_value(eocd32.offset_of_start_of_central_directory);
	nent = byte2_value(eocd32.total_number_of_entries_in_the_central_directory);
    } else {
	struct zip64_end_of_central_directory eocd64 = eocd_record.eocd64;
	cdsiz = byte8_value(eocd64.size_of_the_central_directory);
	cdpos = byte8_value(eocd64.offset_of_start_of_central_directory);
	nent = byte8_value(eocd64.total_number_of_entries_in_the_central_directory);
    }

    assert(eocd_position == (off_t)(cdpos + cdsiz));

    struct eocd_block x = {
	.position = eocd_position,
	.ok = true,
	.zip64 = zip64,
	.cd_size = cdsiz,
	.cd_start = cdpos,
	.cd_n_entries = nent
    };
    return x;
}

/* Gets the values of extended_information by referring to both the
   directory base-entry and the extended_information.  It is called
   with a pointer to an extended_information, when some slots exceeds
   the directory base-entry. */

struct extended_information
copy_extended_information(struct central_directory *e,
			  struct extra_field_header *x)
{
    struct extended_information ei;

    off_t len;
    if (x == 0) {
	len = 0;
    } else {
	assert(byte2_value(x->tag) == 0x0001);
	len = byte2_value(x->length);
    }

    const byte *x0 = (void *)x;
    const byte *p = (x0 + 4);
    off_t position = 0;

    /* uncompressed_size: */

    uint32_t usize = byte4_value(e->uncompressed_size);
    if (usize != 0xffffffff) {
	ei.uncompressed_size = (uint64_t)usize;
	ei.usize = false;
    } else {
	assert(x != 0);
	assert((position + 8) <= len);
	byte8 *q = (void *)(p + position);
	ei.uncompressed_size = byte8_value(*q);
	ei.usize = true;
	position += 8;
    }

    /* compressed_size: */

    uint32_t csize = byte4_value(e->compressed_size);
    if (csize != 0xffffffff) {
	ei.compressed_size = (uint64_t)csize;
	ei.csize = false;
    } else {
	assert(x != 0);
	assert((position + 8) <= len);
	byte8 *q = (void *)(p + position);
	ei.compressed_size = byte8_value(*q);
	ei.csize = true;
	position += 8;
    }

    /* relative_offset_of_local_header: */

    off_t lf = byte4_value(e->relative_offset_of_local_header);
    if (lf != 0xffffffff) {
	ei.relative_offset_of_local_header = (uint64_t)lf;
	ei.lf = false;
    } else {
	assert(x != 0);
	assert((position + 8) <= len);
	byte8 *q = (void *)(p + position);
	ei.relative_offset_of_local_header = byte8_value(*q);
	ei.lf = true;
	position += 8;
    }

    /* number_of_this_disk: */

    uint16_t diskno = byte2_value(e->disk_number_start);
    if (diskno != 0xffff) {
	ei.number_of_this_disk = (uint32_t)diskno;
	ei.diskno = false;
    } else {
	assert(x != 0);
	assert((position + 4) <= len);
	byte4 *q = (void *)(p + position);
	ei.number_of_this_disk = byte4_value(*q);
	ei.diskno = true;
	position += 4;
    }
    assert(position == (off_t)len);

    return ei;
}

/* Makes an extended_information record to store in file.  It return
   the same value that is passed as an argument. */

struct extra_field_header *
make_extended_information(struct extra_field_header *x,
			  struct extended_information ei)
{
    const byte *x0 = (void *)x;
    const byte *p = (x0 + 4);
    off_t position = 0;

    if (ei.usize) {
	byte8 *q = (void *)(p + position);
	*q = byte8_bytes(ei.uncompressed_size);
	position += 8;
    }
    if (ei.csize) {
	byte8 *q = (void *)(p + position);
	*q = byte8_bytes(ei.compressed_size);
	position += 8;
    }
    if (ei.lf) {
	byte8 *q = (void *)(p + position);
	*q = byte8_bytes(ei.relative_offset_of_local_header);
	position += 8;
    }
    if (ei.diskno) {
	byte4 *q = (void *)(p + position);
	*q = byte4_bytes(ei.number_of_this_disk);
	position += 4;
    }

    x->tag = byte2_bytes(0x0001);
    x->length = byte2_bytes(position);

    return x;
}

/* Takes slot values either from a directory base-entry or from an
   extended_information.  It returns the slot values as a fully
   populated extended_information. */

struct extended_information
check_extended_information(struct central_directory *e)
{
    assert(byte4_value(e->signature) == 0x02014b50);
    int file_name_length = byte2_value(e->file_name_length);
    int extra_field_length = byte2_value(e->extra_field_length);
    /*int file_comment_length = byte2_value(e->file_comment_length);*/

    if ((byte4_value(e->compressed_size) != 0xffffffff)
	&& (byte4_value(e->uncompressed_size) != 0xffffffff)
	&& (byte2_value(e->disk_number_start) != 0xffff)
	&& (byte4_value(e->relative_offset_of_local_header) != 0xffffffff)) {

	/* All slots are in the base-entry (zip32). */

	struct extended_information ei0 = copy_extended_information(e, 0);
	return ei0;
    }

    assert(extra_field_length > 0);

    byte *ee0 = (void *)e;
    byte *extra = (void *)(ee0 + (sizeof(struct central_directory)
				  + file_name_length));
    struct extended_information ei1;
    bool exists = false;
    byte *p = extra;
    while (p < (extra + extra_field_length)) {
	struct extra_field_header *x = (void *)p;
	uint16_t id = byte2_value(x->tag);
	uint16_t len = byte2_value(x->length);
	if (verbosity >= 2) {
	    printf("EXTRA_FIELD tag=%x length=%d\n", id, len);
	    fflush(0);
	}
	if (id == 0x0001) {
	    assert(exists == false);
	    exists = true;
	    ei1 = copy_extended_information(e, x);
	}
	p += (len + 4);
    }
    assert(p == (extra + extra_field_length));
    if (exists == false) {
	printf("extended_information needed\n");
	assert(0);
	exit(1);
    }
    return ei1;
}

/* Copies the extra_fields from e0 to e1, but drops the
   extended_information (if exists).  It returns the bytes copied. */

int
copy_extra_fields(struct central_directory *e1, struct central_directory *e0)
{
    assert(byte4_value(e0->signature) == 0x02014b50);
    int file_name_length = byte2_value(e0->file_name_length);
    int extra_field_length = byte2_value(e0->extra_field_length);
    /*int file_comment_length = byte2_value(e0->file_comment_length);*/

    byte *x0 = (void *)e0;
    byte *x1 = (void *)e1;
    size_t basesize = sizeof(struct central_directory) + file_name_length;
    byte *extra0 = x0 + basesize;
    byte *extra1 = x1 + basesize;

    byte *q0 = extra0;
    byte *q1 = extra1;
    bool sawone = false;
    while (q0 < (extra0 + extra_field_length)) {
	struct extra_field_header *x = (void *)q0;
	uint16_t id = byte2_value(x->tag);
	uint16_t len = byte2_value(x->length);
	if (id == 0x0001) {
	    assert(sawone == false);
	    sawone = true;
	    q0 += (len + 4);
	} else {
	    memcpy(q1, q0, (len + 4));
	    q0 += (len + 4);
	    q1 += (len + 4);
	}
    }
    assert(q0 == (extra0 + extra_field_length));
    off_t size = (q1 - extra1);
    assert(0 <= size && size <= (off_t)extra_field_length);
    return (int)size;
}

/* Sets an offset in the extended_information.  It adds a new
   extra_field after copying the directory entry.  It returns a new
   size of a directory entry. */

off_t
set_extended_information(struct central_directory *e1,
			 struct central_directory *e0,
			 struct extended_information ei)
{
    assert(byte4_value(e0->signature) == 0x02014b50);
    int file_name_length = byte2_value(e0->file_name_length);
    int extra_field_length0 = byte2_value(e0->extra_field_length);
    int file_comment_length = byte2_value(e0->file_comment_length);
    size_t oldsize = (sizeof(struct central_directory)
		      + file_name_length + extra_field_length0
		      + file_comment_length);
    const size_t maxsz = sizeof(struct maximum_extended_information);

    ei.lf = (ei.relative_offset_of_local_header >= (uint64_t)UINT32_MAX
	     || force_use_of_extra_field);
    byte buf[maxsz];
    struct extra_field_header *p = make_extended_information((void *)buf, ei);

    if (byte2_value(p->length) == 0) {
	uint32_t oldlf = byte4_value(e0->relative_offset_of_local_header);
	assert(oldlf != 0xffffffff);
	memcpy(e1, e0, oldsize);
	uint64_t lf32 = ei.relative_offset_of_local_header;
	assert(lf32 < (uint64_t)UINT32_MAX);
	e1->relative_offset_of_local_header = byte4_bytes((uint32_t)lf32);
	return oldsize;
    }

    byte *x0 = (void *)e0;
    byte *x1 = (void *)e1;

    if (verbosity >= 2) {
	printf("adding a new extra_field\n");
	fflush(0);
    }

    /* Copy a directory entry up to the file name. */

    size_t basesize = (sizeof(struct central_directory) + file_name_length);
    memcpy(e1, e0, basesize);

    /* Copy the old extra_field except for an extended_information. */

    int extra1 = copy_extra_fields(e1, e0);
    assert((extra_field_length0 - (int)maxsz) <= extra1
	   && extra1 <= extra_field_length0);

    /* Add a new extra_field. */

    assert(byte2_value(p->tag) == (uint16_t)0x0001);
    assert(byte2_value(p->length) != 0);
    int sz = (byte2_value(p->length) + 4);
    memcpy((x1 + basesize + extra1), p, sz);
    int extra_field_length1 = (extra1 + sz);
    e1->extra_field_length = byte2_bytes(extra_field_length1);

    /* Taint the ejected slots. */

    if (ei.lf) {
	e1->relative_offset_of_local_header = byte4_bytes(0xffffffff);
    }

    /* Copy a comment part. */

    byte *c0 = x0 + (basesize + extra_field_length0);
    byte *c1 = x1 + (basesize + extra_field_length1);
    memcpy(c1, c0, file_comment_length);

    return (basesize + extra_field_length1 + file_comment_length);
}

off_t
get_local_file_position(struct central_directory *e)
{
    assert(byte4_value(e->signature) == 0x02014b50);
    /*int file_name_length = byte2_value(e->file_name_length);*/
    /*int extra_field_length = byte2_value(e->extra_field_length);*/
    /*int file_comment_length = byte2_value(e->file_comment_length);*/

    off_t lf;
    uint32_t lf32 = byte4_value(e->relative_offset_of_local_header);
    if (lf32 != 0xffffffff) {
	/* An offset resides in the zip32 central_directory. */
	lf = lf32;
    } else {
	/* An offset resides in an extra_field. */
	struct extended_information ei = check_extended_information(e);
	lf = ei.relative_offset_of_local_header;
    }
    return lf;
}

/* The fields .zip_file_name and .zip_file_size can be empty yet. */

void
dump_central_directory_entry(struct central_directory *e)
{
    if (verbosity < 2) {
	return;
    }

    byte *p = (void *)e;
    assert(byte4_value(e->signature) == 0x02014b50);
    int file_name_length = byte2_value(e->file_name_length);
    int extra_field_length = byte2_value(e->extra_field_length);
    int file_comment_length = byte2_value(e->file_comment_length);

    char *s = (void *)(p + sizeof(struct central_directory));
    char ss[80];
    int len = MIN(file_name_length, (int)(sizeof(ss) - 1));
    memcpy(ss, s, len);
    ss[len] = 0;
    printf("** file_name=(%s)\n", ss);

    uint16_t v0 = byte2_value(e->version_made_by);
    int attributemapping0 = (v0 >> 8);
    int version0 = (v0 & 0xff);
    printf("version_made_by=%d:%d.%d\n",
	   attributemapping0, version0/10, version0%10);
    uint16_t v1 = byte2_value(e->version_needed_to_extract);
    int attributemapping1 = (v1 >> 8);
    int version1 = (v1 & 0xff);
    printf("version_needed_to_extract=%d:%d.%d\n",
	   attributemapping1, version1/10, version1%10);
    /*byte general_purpose_bit_flag[2];*/
    /*byte compression_method[2];*/
    /*byte last_mod_file_time[2];*/
    /*byte last_mod_file_date[2];*/
    printf("crc32=%x\n", byte4_value(e->crc32));
    printf("compressed_size=%u\n", byte4_value(e->compressed_size));
    printf("uncompressed_size=%u\n", byte4_value(e->uncompressed_size));
    /*byte file_name_length[2];*/
    /*byte extra_field_length[2];*/
    /*byte file_comment_length[2];*/
    /*byte disk_number_start[2];*/
    /*byte internal_file_attributes[2];*/
    /*byte external_file_attributes[4];*/

    printf("file_name_length=%d\n", file_name_length);
    printf("extra_field_length=%d\n", extra_field_length);
    printf("file_comment_length=%d\n", file_comment_length);

    off_t lf = get_local_file_position(e);
    printf("relative_offset_of_local_header=%ld\n", lf);

    p += (sizeof(struct central_directory)
	  + file_name_length + extra_field_length + file_comment_length);

    fflush(0);
}

/* Reads and returns a central_directory.  It returns a record
   malloced.  It scans the directory and optionally dumps each
   entry. */

struct cd_block
read_central_directory(int fd, off_t pos, size_t siz, size_t n_entries,
		       __attribute__((unused)) char *filename)
{
    byte *p = malloc(siz);
    if (p == 0) {
	perror("malloc central_directory");
	exit(1);
    }
    ssize_t ss = pread_whole(fd, p, siz, pos);
    if (ss == -1 || ss != (ssize_t)siz) {
	perror("pread central_directory");
	assert(0);
    }

    const size_t nent = n_entries;
    const byte *limit = (byte *)(p + siz);
    byte *q = p;
    for (size_t i = 0; i < nent; i++) {
	assert(q < limit);
	struct central_directory *d = (void *)q;
	assert(byte4_value(d->signature) == 0x02014b50);
	int file_name_length = byte2_value(d->file_name_length);
	int extra_field_length = byte2_value(d->extra_field_length);
	int file_comment_length = byte2_value(d->file_comment_length);

	dump_central_directory_entry(d);

	q += (sizeof(struct central_directory)
	      + file_name_length + extra_field_length + file_comment_length);
    }
    if (q == (p + siz)) {
	/* No digital signature. */
    } else {
	struct digital_signature *g = (void *)q;
	uint32_t m = byte4_value(g->signature);
	assert(m == 0x05054b50);
    }

    struct cd_block dd;
    dd.directory = (void *)p;
    dd.zip_file_name = /*dummy*/ 0;
    dd.zip_file_size = /*dummy*/ 0;
    dd.n_entries = n_entries;
    dd.directory_size = (q - p);
    dd.directory_size_full = siz;
    return dd;
}

/* Translates the directory entries of the input file to be positioned
   at a specified position in the output file.  Thus, it adds the
   value of position to the offsets.  It replaces the directory record
   by a new one, because the size of each record may increase.  It
   drops a digital_signature.  It allocates a new directory with
   a slack. */

void
translate_directory_entries(struct cd_block *dd, off_t position)
{
    const int sz = (int)sizeof(struct maximum_extended_information);
    const size_t n_entries = dd->n_entries;
    byte *oldd = (void *)dd->directory;
    size_t size = (dd->directory_size + sz * n_entries);
    byte *newd = malloc(size);
    if (newd == 0) {
	perror("malloc central_directory");
	exit(1);
    }
    memset(newd, 0, size);

    size_t directory_size = dd->directory_size;
    const byte *limit0 = (byte *)oldd + dd->directory_size;
    const byte *limit1 = (byte *)newd + size;
    byte *p0 = oldd;
    byte *p1 = newd;
    for (size_t i = 0; i < n_entries; i++) {
	assert(p0 < limit0 && p1 < limit1);
	struct central_directory *e0 = (void *)p0;
	struct central_directory *e1 = (void *)p1;
	assert(byte4_value(e0->signature) == 0x02014b50);
	int file_name_length = byte2_value(e0->file_name_length);
	int extra_field_length0 = byte2_value(e0->extra_field_length);
	int file_comment_length = byte2_value(e0->file_comment_length);
	assert((uint64_t)extra_field_length0 + sz < (uint64_t)UINT32_MAX);
	size_t oldsize = (sizeof(struct central_directory)
			  + file_name_length + extra_field_length0
			  + file_comment_length);

	/* FIX THE LOCAL_FILE OFFSET. */

	/*off_t lf0 = get_local_file_position(e0);*/
	/*assert(lf0 != -1);*/
	struct extended_information ei = check_extended_information(e0);
	off_t lf0 = ei.relative_offset_of_local_header;
	off_t lf1 = lf0 + position;
	if (verbosity >= 1) {
	    char *s = (void *)(p0 + sizeof(struct central_directory));
	    char ss[80];
	    int len = MIN(file_name_length, (int)(sizeof(ss) - 1));
	    memcpy(ss, s, len);
	    ss[len] = 0;
	    printf("modifying offset (%s) %ld to %ld\n", ss, lf0, lf1);
	    fflush(0);
	}

	ei.relative_offset_of_local_header = lf1;
	size_t newsize = set_extended_information(e1, e0, ei);
	directory_size += (newsize - oldsize);
	assert(newsize >= oldsize);

	p0 += oldsize;
	p1 += newsize;
    }

    assert(directory_size <= size);
    dd->directory = (void *)newd;
    dd->directory_size = directory_size;
    dd->directory_size_full = directory_size;
    free(oldd);
}

/* Writes a new central directory entries at the current file-pointer
   position.  It writes the "central_directory" and records of
   "zip64_end_of_central_directory",
   "zip64_end_of_central_directory_locator", and
   "end_of_central_directory".  But, it omits the first two records
   "archive_decryption_header" and "archive_extra_data_record" which
   go ahead of the central_directory.  The locater (offset) in
   zip64_end_of_central_directory_locator points to the eocd64.  It
   moves the file-pointer to the end of the file by itself, because
   pwrite(2) does not move it. */

bool
write_directory(int ofd, int n_files, struct cd_block *directories,
		__attribute__((unused)) char *filename)
{
    const bool WITH_EOCD64 = true;
    assert((sizeof(struct zip64_end_of_central_directory)
	    + sizeof(struct zip64_end_of_central_directory_locator)) == 76);

    size_t ent = 0;
    size_t size = 0;
    off_t pos = 0;
    for (int j = 0; j < n_files; j++) {
	struct cd_block *dd = &directories[j];
	translate_directory_entries(dd, pos);
	ent += dd->n_entries;
	size += dd->directory_size;
	pos += dd->zip_file_size;
    }
    const off_t cdstart = pos;
    const size_t cdsize = size;
    const size_t cdcount = ent;

    if (verbosity >= 1) {
	printf("directory start=%ld size=%ld entry-count=%ld\n",
	       cdstart, cdsize, cdcount);
	fflush(0);
    }

    struct stat st;
    int cc0 = fstat(ofd, &st);
    assert(cc0 != -1);

    assert(cdstart == st.st_size);

    off_t pos0 = lseek(ofd, cdstart, SEEK_SET);
    assert(pos0 != -1);

    /* archive_decryption_header */
    /* archive_extra_data_record */

    /* central_directory x N times. */

    for (int i = 0; i < n_files; i++) {
	struct cd_block dd = directories[i];
	ssize_t ss2 = write_whole(ofd, dd.directory, dd.directory_size);
	assert(ss2 != -1);
	if (verbosity >= 2) {
	    printf("directory block size=%ld size+signature=%ld\n",
		   dd.directory_size, dd.directory_size_full);
	    fflush(0);
	}
    }

    /* zip64_end_of_central_directory. */

    const off_t eocd64pos = (cdstart + cdsize);
    off_t pos1 = lseek(ofd, 0, SEEK_CUR);
    assert(pos1 == eocd64pos);

    if (WITH_EOCD64) {
	const uint32_t marker = 0x06064b50;
	const uint64_t eocd_size = sizeof(struct zip64_end_of_central_directory);

	/* The size is for empty zip64_extensible_data_sector, and it
	   does not include first 12 bytes. */

	struct zip64_end_of_central_directory eocd64 = {
	    .signature = byte4_bytes(marker),
	    .size_of_zip64_end_of_central_directory
	    = byte8_bytes(eocd_size - 12),
	    .version_made_by.b = {
		[0] = 30, [1] = 3},
	    .version_needed_to_extract.b = {
		[0] = 45, [1] = 0},
	    .number_of_this_disk.b = {
		0, 0, 0, 0},
	    .number_of_the_disk_with_the_start_of_the_central_directory.b = {
		0, 0, 0, 0},
	    .total_number_of_entries_in_the_central_directory_on_this_disk
	    = byte8_bytes(cdcount),
	    .total_number_of_entries_in_the_central_directory
	    = byte8_bytes(cdcount),
	    .size_of_the_central_directory
	    = byte8_bytes(cdsize),
	    .offset_of_start_of_central_directory
	    = byte8_bytes(cdstart),
	    /*zip64_extensible_data_sector*/
	};
	ssize_t ss2 = write_whole(ofd, &eocd64, sizeof(eocd64));
	assert(ss2 != -1);
    }

    /* zip64_end_of_central_directory_locator. */

    if (WITH_EOCD64) {
	const uint32_t marker = 0x07064b50;
	struct zip64_end_of_central_directory_locator ecdl = {
	    .signature = byte4_bytes(marker),
	    .number_of_the_disk.b = {
		0, 0, 0, 0},
	    .relative_offset_of_the_zip64_end_of_central_directory
	    = byte8_bytes(eocd64pos),
	    .total_number_of_disks.b = {
		1, 0, 0, 0},
	};
	ssize_t ss3 = write_whole(ofd, &ecdl, sizeof(ecdl));
	assert(ss3 != -1);
    }

    /* end_of_central_directory. */

    {
	const uint32_t marker = 0x06054b50;
	uint32_t cdstart32 = (uint32_t)MIN((off_t)0xffffffff, cdstart);
	uint32_t cdsize32 = (uint32_t)MIN((size_t)0xffffffff, cdsize);
	uint16_t cdcount16 = (uint16_t)MIN((size_t)0xffff, cdcount);
	uint16_t thisdisk = WITH_EOCD64 ? 0 : 0;

	struct end_of_central_directory eocd32 = {
	    .signature = byte4_bytes(marker),
	    .number_of_this_disk = byte2_bytes(thisdisk),
	    .number_of_the_disk_with_the_start_of_the_central_directory.b = {
		0, 0},
	    .total_number_of_entries_in_the_central_directory_on_this_disk
	    = byte2_bytes(cdcount16),
	    .total_number_of_entries_in_the_central_directory
	    = byte2_bytes(cdcount16),
	    .size_of_the_central_directory
	    = byte4_bytes(cdsize32),
	    .offset_of_start_of_central_directory
	    = byte4_bytes(cdstart32),
	    .zip_file_comment_length.b = {
		0, 0}
	    /* zip_file_comment[zip_file_comment_length] */
	};
	ssize_t ss4 = write_whole(ofd, &eocd32, sizeof(eocd32));
	assert(ss4 != -1);
    }

    return 0;
}

int
main(int argc, char *argv[])
{
    assert(sizeof(off_t) == 8);
    assert(sizeof(byte2) == 2);
    assert(sizeof(byte4) == 4);
    assert(sizeof(byte8) == 8);

    /* Checks "byteX" does not restrict alignment. */
    assert(sizeof(struct {byte a; byte8 b;}) == 9);
    assert(sizeof(struct {byte a; byte8 b; byte2 c;}) == 11);

    assert(sizeof(struct local_file_record) == 30);
    assert(sizeof(struct data_descriptor32) == 12);
    assert(sizeof(struct data_descriptor64) == 20);
    assert(sizeof(struct central_directory) == 46);
    assert(sizeof(struct digital_signature) == 6);
    assert(sizeof(struct zip64_end_of_central_directory) == 56);
    assert(sizeof(struct zip64_end_of_central_directory_locator) == 20);
    assert(sizeof(struct end_of_central_directory) == 22);
    assert(sizeof(struct extra_field_header) == 4);
    assert(sizeof(struct maximum_extended_information) == 32);

    if (argc >= 2 && strcmp(argv[1], "--version") == 0) {
	printf("splicezip version 2021-06-22\n");
	exit(0);
    }

    int c;
    extern char *optarg;
    extern int opterr, optind, optopt;

    while ((c = getopt(argc, argv, "hvqEW")) != EOF) {
	switch (c) {
	case -1:
	    break;
	case 'h':
	    goto USAGE;
	case 'v':
	    verbosity = MIN(4, (verbosity + 1));
	    break;
	case 'q':
	    verbosity = 0;
	    break;
	case 'E':
	    force_use_of_extra_field = true;
	    break;
	case 'W':
	    wipe_off_old_directory = true;
	    break;
	case '?':
	default:
	    goto USAGE;
	}
    }
    assert(optind <= argc);

    if ((optind + 2) > argc) {
	USAGE:
	printf("USAGE: %s [-vq] out-zip-file in-zip-files...\n",
	       argv[0]);
	exit(1);
    }

    int n_files = (argc - (optind + 1));
    struct cd_block *file_blocks = malloc(n_files * sizeof(struct cd_block));
    assert(file_blocks != 0);
    memset(file_blocks, 0, (n_files * sizeof(struct cd_block)));

    /* Using O_DIRECT on an output file requires writes are aligned to
       (long) st_blksize.  (It is not likely a strict requirement on
       Linux). */

    /* O_TRUNC or O_EXCL?? */

    char *out_file_name = argv[optind];
    mode_t mode = 0666;
    int ofd = open(out_file_name, (O_WRONLY|O_CREAT|O_TRUNC), mode);
    if (ofd == -1) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, sizeof(ee), "open(%s): %s", out_file_name, m);
	fprintf(stderr, "%s\n", ee);
	exit(1);
    }
    int cc3 = posix_fadvise(ofd, 0, 0, POSIX_FADV_NOREUSE);
    assert(cc3 == 0);
    if (1) {
	struct stat st;
	int cc0 = fstat(ofd, &st);
	assert(cc0 != -1);
	assert((st.st_mode & S_IFMT) == S_IFREG);

	struct statvfs vfs;
	int cc1 = fstatvfs(ofd, &vfs);
	assert(cc1 != -1);

	/*printf("st_blksize=%d f_bsize=%ld\n", st.st_blksize, vfs.f_bsize);*/
	/*fflush(0);*/
    }

    off_t out_file_position = 0;

    for (int i = 0; i < n_files; i++) {
	assert((optind + 1 + i) < argc);
	char *file_name = argv[(optind + 1 + i)];

	if (verbosity >= 1) {
	    printf("*** ZIP-FILE=%s \n", file_name);
	    fflush(0);
	}

	int fd = open(file_name, O_RDONLY, 0);
	if (fd == -1) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "open(%s): %s", file_name, m);
	    fprintf(stderr, "%s\n", ee);
	    exit(1);
	}

	struct stat st;
	int cc0 = fstat(fd, &st);
	if (cc0 == -1) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "fstat(%s): %s", file_name, m);
	    fprintf(stderr, "%s\n", ee);
	    exit(1);
	}
	assert((st.st_mode & S_IFMT) == S_IFREG);
	/*file_sizes[i] = st.st_size;*/
	/*printf("st_blksize=%d\n", st.st_blksize);*/

	struct eocd_block eocd = find_eocd(fd, file_name);
	assert(eocd.ok);
	off_t cdpos = eocd.cd_start;
	size_t cdsiz = eocd.cd_size;
	size_t nent = eocd.cd_n_entries;
	if (verbosity >= 2) {
	    printf("EOCD offset=%ld zip64=%d directory start=%ld end=%ld\n",
		   eocd.position, eocd.zip64, cdpos, (cdpos + cdsiz));
	    fflush(0);
	}

	struct cd_block dd = read_central_directory(fd, cdpos, cdsiz, nent, file_name);
	dd.zip_file_name = file_name;
	dd.zip_file_size = st.st_size;
	file_blocks[i] = dd;

	/* (lseek is unnecessary to mmap). */
	off_t cc2 = lseek(fd, 0, SEEK_SET);
	assert(cc2 != -1);
	size_t len = st.st_size;
	int prot = (! wipe_off_old_directory
		    ? PROT_READ : (PROT_READ|PROT_WRITE));
	int flags = (MAP_PRIVATE|MAP_NORESERVE);
	byte *contents = (void *)mmap(0, len, prot, flags, fd, 0);
	assert(contents != 0);

	/* Wipe off the original directory in the input zip file (for
	   testing). */

	if (prot == (PROT_READ|PROT_WRITE)) {
	    /*memset((contents + cdpos), 0, cdsiz);*/
	    memset((contents + cdpos), 0, (len - cdpos));
	}

	ssize_t ss2 = pwrite_whole(ofd, contents, len, out_file_position);
	assert(ss2 != -1);

	int cc4 = munmap((void *)contents, len);
	assert(cc4 == 0);
	int cc1 = close(fd);
	assert(cc1 == 0);

	out_file_position += len;
    }

    {
	int cc = write_directory(ofd, n_files, file_blocks, out_file_name);
	assert(cc == 0);
    }

    return 0;
}
