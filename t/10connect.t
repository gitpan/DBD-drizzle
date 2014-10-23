#!perl -w
# vim: ft=perl

use Test::More ;
use DBI;
use DBI::Const::GetInfoType;
use strict;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
print "test_dsn $test_dsn\n";
eval {$dbh= DBI->connect("$test_dsn;port=4427", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr Can't continue test $@";
}
plan tests => 2; 

ok defined $dbh, "Connected to database";

ok $dbh->disconnect();
