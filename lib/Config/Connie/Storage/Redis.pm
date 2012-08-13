package Config::Connie::Storage::Redis;

# ABSTRACT: Redis-based storage helper for Config::Connie
# VERSION
# AUTHORITY

use Moo;
use JSON qw( encode_json decode_json );
use namespace::autoclean;

extends 'Config::Connie::Storage';


#######################################
# Redis connection management and cache

has 'redis_connect' => (is => 'ro', required => 1);

has '_redis_cmds'   => (is => 'lazy');
has '_redis_pubsub' => (is => 'lazy');

sub _build__redis_cmds   { $_[0]->redis_connect->($_[0], 'cmds') }
sub _build__redis_pubsub { $_[0]->redis_connect->($_[0], 'pubsub') }


######################
# Lifecycle management

sub init {
  my $self = shift;

  ## Note well: order is important - we need the subscriptions active
  ## before we init the local cache, to make sure we don't lose updates
  $self->_init_subscriptions;
  $self->_init_local_cache;
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


#############################
# Bootstrap local config keys

sub _init_local_cache {
  my ($self) = @_;

  my $keys = $self->_redis_cmds->zrange($self->all_keys_set, 0, -1);
  $self->_on_key_update_notifcation($_) for @$keys;
}


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
