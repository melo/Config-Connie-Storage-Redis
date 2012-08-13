package T::Config;

use Moo;
use Redis;
use Config::Connie::Storage::Redis;
use namespace::autoclean;

with 'Config::Connie';

has 'redis_port' => (is => 'ro', required => 1);

sub default_config_id {'redis_storage_id'}

sub build_storage {
  my ($self) = @_;

  Config::Connie::Storage::Redis->new(
    instance      => $self,
    redis_connect => sub {
      Redis->new(
        server   => "127.0.0.1:" . $self->redis_port,
        encoding => undef,
      );
    }
  );

}


1;
