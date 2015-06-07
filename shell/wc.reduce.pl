#!/usr/bin/perl

# wc.reduce.pl.  Example for KMR shell command pipeline.  It is a
# reducer for word count.

use strict;

my $counter;

while(<>) {
	chomp $_;
	my ($key, $value) = split " ";
	$counter->{$key} += $value;
}

for (sort { $counter->{$a} <=> $counter->{$b} } keys %$counter) {
	print $_, " ", $counter->{$_}, "\n";
}
