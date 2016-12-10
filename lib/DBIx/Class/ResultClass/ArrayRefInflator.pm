package DBIx::Class::ResultClass::ArrayRefInflator;
use Data::Dump qw/dump/;

sub inflate_result {
    # dump \@_;
    # dump $_[3];
    $_[2];
    # return $self;
    # return [ map { ref $_ ? ref $_ : $_ } @_ ];
    return $_[2]
}

1;
