package Config::Connie::Storage::Redis;

# ABSTRACT: Redis-based storage helper for Config::Connie
# VERSION
# AUTHORITY

use Moo;
use JSON qw( encode_json decode_json );
use namespace::autoclean;

with
  'Config::Connie::Storage::Core',
  'Config::Connie::Storage::Version',
  ;


#######################################
# Redis connection management and cache

has 'redis_connect' => (is => 'ro', required => 1);

has '_redis_cmds'   => (is => 'lazy');
has '_redis_pubsub' => (is => 'lazy');

sub _build__redis_cmds   { $_[0]->redis_connect->($_[0], 'cmds') }
sub _build__redis_pubsub { $_[0]->redis_connect->($_[0], 'pubsub') }


#########################
# Setup and update checks

sub init {
  my $self = shift;

  ## Note well: order is important - we need the subscriptions active
  ## before we init the local cache, to make sure we don't lose updates
  $self->_init_subscriptions;
  $self->_init_local_cache;
  $self->version;
}

sub check_for_updates {
  return shift->_redis_pubsub->wait_for_messages(0);
}


##########################
# Deal with client updates

sub key_updated {
  my ($self, $k, $v) = @_;
  my $payload = __encode_value($k, $v);

  my $redis = $self->_redis_cmds;
  $redis->set($self->key_for_cfg_key($k), $payload);

  $self->_update_storage_version;

  $redis->zadd($self->all_keys_set, time(), $k);
  $redis->publish($self->notification_topic,     $k);
  $redis->publish($self->fat_notification_topic, $payload);
}


############
# Redis keys

has 'prefix' => (is => 'ro', default => sub {'connie_cfg'});

sub _build_redis_key { my $s = shift; join('|', $s->prefix, $s->instance->id, @_) }
sub notification_topic     { $_[0]->_build_redis_key }
sub fat_notification_topic { $_[0]->_build_redis_key('fat') }
sub all_keys_set           { $_[0]->_build_redis_key('idx') }
sub key_for_cfg_key        { $_[0]->_build_redis_key('keys', $_[1]) }


#############################
# Bootstrap local config keys

sub _init_local_cache {
  my ($self) = @_;

  my $cmds = $self->_redis_cmds;
  my $keys = $cmds->zrange($self->all_keys_set, 0, -1);

  return unless @$keys;

  my $vals = $cmds->mget(map { $self->key_for_cfg_key($_) } @$keys);
  $self->_on_key_update_notifcation($_) for @$vals;
}


###############
# Notifications

sub _init_subscriptions {
  my ($self) = @_;

  my $redis = $self->_redis_pubsub;
  $redis->subscribe($self->fat_notification_topic, sub { $self->_on_key_update_notifcation(shift) });
}

sub _on_key_update_notifcation {
  my ($self, $v) = @_;

  if ($v) {
    $v = __decode_value($v);
    $self->instance->_cache_updated($v->{key} => $v->{cfg});
  }
}


###############
# Encode/decode

sub __decode_value { decode_json($_[0]) }
sub __encode_value { encode_json({ key => $_[0], cfg => $_[1] }) }


#########
# Version

sub version_key { $_[0]->_build_redis_key('version') }

sub get_storage_version {
  my ($self) = @_;

  $self->_redis_cmds->get($self->version_key);
}

sub _update_storage_version {
  my ($self) = @_;

  $self->_set_version($self->_redis_cmds->incr($self->version_key));
}


1;
