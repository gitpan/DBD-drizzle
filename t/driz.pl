#!/usr/bin/perl -w

use strict;
use Data::Dumper;
use DBI;

my $dbh= DBI->connect('DBI:drizzle:test', '', '') or die "Unable to connect! $@\n";

$dbh->do('drop table if exists t1');
$dbh->do('create table t1 (a int)');

my $sth= $dbh->prepare('insert into t1 values (1)');

$sth->execute();

$sth= $dbh->prepare('select * from t1');
$sth->execute();

my $retref= $sth->fetchall_arrayref();

print Dumper $retref;
