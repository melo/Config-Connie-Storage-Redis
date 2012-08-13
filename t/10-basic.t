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

subtest 'get/set' => sub {
  is($i->get('he'), undef, 'get() returns undef for unkown keys');
  is($i->set('he', 'human'), 'human', 'set() returns setted value');
  is($i->get('he'), undef, '... but local cache not immediatly updated');

  usleep(100) until $i->check_for_updates;
  is($i->get('he'), 'human', 'Cache updated after check_for_updates');

  ## clear local cache with a new T::Config instance
  $i = T::Config->setup(redis_port => $port);

  is($i->get('he'), 'human', 'Local cache updated after reconnect to storage');
};


subtest 'config changes' => sub {
  my $i = T::Config->setup(redis_port => $port);

  my $cfg1;
  my $cb1 = sub { my ($v, $k) = @_; $cfg1 = { $k => $v } };
  my $id1 = $i->subscribe('x1' => $cb1);
  ok($id1, 'subscribe() returns a true subscription ID');

  $i->set('x1' => 'y1');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y1' }, 'set() calls registered subscribers');

  $i->set('x2' => 'y2');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y1' }, '... but only matching our subscriber key');

  my $cfg2;
  my $cb2 = sub { my ($v, $k) = @_; $cfg2 = { $k => $v } };
  my $id2 = $i->subscribe('x1' => $cb2);
  ok($id2, 'subscribe() returns a true subscription ID');

  $i->set('x1' => 'y3');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y3' }, 'set() calls registered subscribers');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '... all subscribers are called');

  my $cfg3;
  my $cb3 = sub { my ($v, $k) = @_; $cfg3 = { $k => $v } };
  my $id3 = $i->subscribe('y1' => $cb3);
  ok($id3, 'subscribe() returns a true subscription ID');

  $i->set('y1' => 'z1');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y3' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  registered subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  is($i->unsubscribe($id2), $cb2,  'unsubscribe() returns the callback');
  is($i->unsubscribe($id2), undef, '... or undef if subscription ID is not valid/found');

  $i->set('x1' => 'y4');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y4' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  active subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  is($i->unsubscribe($id1), $cb1, 'unsubscribe() returns the callback, again');

  $i->set('x1' => 'y5');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y4' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  active subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  is($i->unsubscribe($id3), $cb3, 'unsubscribe() returns the callback, again');

  $i->set('y1' => 'z2');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y4' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  active subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');
};


done_testing();
