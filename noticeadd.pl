#!/usr/bin/perl
# noticeadd.pl (from gridmpi packager)

use File::Find;

# main routine

print "Replacing LICENSE NOTICE...\n";

open(NOTICE, "< ./notice.txt");
@notice = <NOTICE>;
close(NOTICE);

for ($i = 0; $i <= $#notice; $i++) {
    $noticesh[$i] = "# " . $notice[$i];
    $noticef90[$i] = "! " . $notice[$i];
}

find(\&dofile, "src");
find(\&dofile, "kmrrun");
find(\&dofile, "cmd");
find(\&dofile, "shell");
find(\&dofile, "ex");

# change notice

sub dofile {
    $modified = 0;
    open(FILE, "+< $_");
    @lines = <FILE>;
    if ($_ =~ /^\./) {
	# ignore
    } else {
	print "$_ ...";
	for ($i = 0; $i <= $#lines; $i++) {
	    if ($lines[$i] =~ /^NOTICE-NOTICE-NOTICE$/) {
		splice(@lines, $i, 1, @notice);
		$modified = 1;
	    } elsif ($lines[$i] =~ /^! NOTICE-NOTICE-NOTICE-F90$/) {
		splice(@lines, $i, 1, @noticef90);
		$modified = 1;
	    } elsif ($lines[$i] =~ /^\# NOTICE-NOTICE-NOTICE-SH$/) {
		splice(@lines, $i, 1, @noticesh);
		$modified = 1;
	    } else {
		next;
	    }
	}
    }
    if ($modified) {
	seek(FILE, 0, 0);
	print FILE @lines;
	print " *\n";
    } else {
	print "\n";
    }
    close(FILE);
}
