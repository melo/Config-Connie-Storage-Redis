#!perl

use lib 't/tlib';
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Config::Connie;
use Config::Connie::Storage::Redis;
use Redis;
use My::Test::SpawnRedis;
use Time::HiRes 'usleep';

my $redis_ctl = My::Test::SpawnRedis->start;
my $port      = $redis_ctl->port;

my $i = Config::Connie->register(
  app => 'my_app',
  env => 'test',

  storage_builder => sub {
    Config::Connie::Storage::Redis->new(
      @_,
      redis_connect => sub {
        Redis->new(
          server   => "127.0.0.1:$port",
          encoding => undef,
        );
      }
    );
  }
);

subtest 'get/set' => sub {
  my $c = $i->client;

  is($c->get('he'), undef, 'get() returns undef for unkown keys');
  is($c->set('he', 'human'), 'human', 'set() returns setted value');
  is($c->get('he'), undef, '... but local cache not immediatly updated');

  usleep(100) until $i->check_for_updates;
  is($c->get('he'), 'human', 'Cache updated after check_for_updates');

  ## clear local cache
  my $cfg = $c->cfg;
  delete $cfg->{$_} for keys %$cfg;
  is($c->get('he'), undef, 'get() returns undef after local cache wipe');

  ## Force disconnect/reconnect and init of storage link
  delete $c->{storage};
  $c->storage;

  is($c->get('he'), 'human', 'Local cache updated after reconnect to storage');
};


subtest 'config changes' => sub {
  my $c = $i->client;

  my $cfg1;
  my $cb1 = sub { my ($v, $k) = @_; $cfg1 = { $k => $v } };
  my $id1 = $c->subscribe('x1' => $cb1);
  ok($id1, 'subscribe() returns a true subscription ID');

  $c->set('x1' => 'y1');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y1' }, 'set() calls registered subscribers');

  $c->set('x2' => 'y2');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y1' }, '... but only matching our subscriber key');

  my $cfg2;
  my $cb2 = sub { my ($v, $k) = @_; $cfg2 = { $k => $v } };
  my $id2 = $c->subscribe('x1' => $cb2);
  ok($id2, 'subscribe() returns a true subscription ID');

  $c->set('x1' => 'y3');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y3' }, 'set() calls registered subscribers');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '... all subscribers are called');

  my $cfg3;
  my $cb3 = sub { my ($v, $k) = @_; $cfg3 = { $k => $v } };
  my $id3 = $c->subscribe('y1' => $cb3);
  ok($id3, 'subscribe() returns a true subscription ID');

  $c->set('y1' => 'z1');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y3' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  registered subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  is($c->unsubscribe($id2), $cb2,  'unsubscribe() returns the callback');
  is($c->unsubscribe($id2), undef, '... or undef if subscription ID is not valid/found');

  $c->set('x1' => 'y4');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y4' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  active subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  is($c->unsubscribe($id1), $cb1, 'unsubscribe() returns the callback, again');

  $c->set('x1' => 'y5');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y4' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  active subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  is($c->unsubscribe($id3), $cb3, 'unsubscribe() returns the callback, again');

  $c->set('y1' => 'z2');
  usleep(100) until $i->check_for_updates;
  cmp_deeply($cfg1, { 'x1' => 'y4' }, 'set() only calls...');
  cmp_deeply($cfg2, { 'x1' => 'y3' }, '...  active subscribers ...');
  cmp_deeply($cfg3, { 'y1' => 'z1' }, '...  that match our key');

  ### Just make sure we cleanup after ourselfs
  cmp_deeply($c->_subs, { i => {}, k => {} }, 'subscription database is empty');
};


done_testing();
