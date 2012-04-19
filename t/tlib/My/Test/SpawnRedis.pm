package My::Test::SpawnRedis;

use strict;
use warnings;
use Test::TCP;
use Test::More;
use FindBin;

sub start {
  my $server;
  if ($server = $ENV{TEST_SERVER_REDIS}) {
    plan skip_all => "redis-server not found at '$server' (via TEST_SERVER_REDIS)"
      unless -e $server && -x _;
  }
  else {
    chomp($server = `which redis-server`);
    plan skip_all => 'redis-server not found in your PATH'
      unless $server && -e $server && -x _;
  }

  return Test::TCP->new(
    code => sub {
      my $port = shift;
      rewrite_redis_conf($port);
      exec($server, "t/tlib/redis.conf");
    },
  );
}

sub rewrite_redis_conf {
  my $port = shift;
  my $dir  = $FindBin::Bin;

  open my $in,  "<", "t/tlib/redis.conf.base" or die $!;
  open my $out, ">", "t/tlib/redis.conf"      or die $!;

  while (<$in>) {
    s/__PORT__/$port/;
    s/__DIR__/$dir/;
    print $out $_;
  }
}

END { unlink for "t/tlib/redis.conf", "t/tlib/dump.rdb" }

1;
