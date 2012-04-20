package Config::Connie::Storage::Redis;

# ABSTRACT: Redis-based storage helper for Config::Connie
# VERSION
# AUTHORITY

use Config::Connie::Object;
use JSON qw( encode_json decode_json );

extends 'Config::Connie::Storage';


#######################################
# Redis connection management and cache

has 'redis_connect' => (is => 'ro', required => 1);

has '_redis_cmds'   => (is => 'ro', default => sub { $_[0]->redis_connect->($_[0], 'cmds') });
has '_redis_pubsub' => (is => 'ro', default => sub { $_[0]->redis_connect->($_[0], 'pubsub') });


######################
# Lifecycle management

sub init {
  my $self = shift;

  $self->_init_subscriptions;
}

sub check_for_updates {
  return shift->_redis_pubsub->wait_for_messages(0);
}


############
# Redis keys

has 'prefix' => (is => 'ro', default => sub {'connie_cfg'});

sub _build_redis_key { my $s = shift; join('|', $s->prefix, $s->client->id, @_) }
sub notification_topic { $_[0]->_build_redis_key }
sub all_keys_set       { $_[0]->_build_redis_key('idx') }
sub key_for_cfg_key    { $_[0]->_build_redis_key('keys', $_[1]) }


###############
# Notifications

sub _init_subscriptions {
  my ($self) = @_;

  my $redis = $self->_redis_pubsub;
  $redis->subscribe($self->notification_topic, sub { $self->_on_key_update_notifcation(shift) });
}

sub _on_key_update_notifcation {
  my ($self, $k) = @_;

  my $v = $self->_redis_cmds->get($self->key_for_cfg_key($k));
  $v = decode_json($v) if $v;
  $v = $v->{cfg}       if $v;

  $self->client->_update_key($k => $v);
}


##############
# Client hooks

sub key_updated {
  my ($self, $k, $v) = @_;

  my $redis = $self->_redis_cmds;
  $redis->set($self->key_for_cfg_key($k), encode_json({ key => $k, cfg => $v }));
  $redis->zadd($self->all_keys_set, time(), $k);
  $redis->publish($self->notification_topic, $k);
}


1;
