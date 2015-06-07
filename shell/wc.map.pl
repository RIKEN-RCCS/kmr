#!/usr/bin/perl

# wc.map.pl.  Example for KMR shell command pipeline.  It is a mapper
# for word count.

use strict;

my $counter;

while(<STDIN>) {
	chomp $_;
	my @w = split(" ", $_);
	foreach my $wd (@w) {
		print $wd, " 1\n";
	}
}
