#!perl

use lib 't/tlib';
use strict;
use warnings;
use Test::More;
use Test::Deep;
use T::SpawnRedis;
use T::Config;
use Time::HiRes 'usleep';

my $redis_ctl = T::SpawnRedis->start;
my $port      = $redis_ctl->port;
my $i         = T::Config->setup(redis_port => $port);

is($i->version, undef, 'initial version is undef, this redis-server is short lived');

$i->set(k => 42);
is($i->version, 1, 'after one set(), version is 1');

$i->set(k => 42);
is($i->version, 2, 'after second set(), version is 2');

done_testing();
